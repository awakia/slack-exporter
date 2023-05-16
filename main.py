# this bot needs permissions below:
# channels:history,channels:join,channels:read

import csv
import datetime
import os
import time
from typing import Optional
import psycopg2
from psycopg2 import extras
from dataclasses import dataclass, field
import argparse

import pytz
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


@dataclass
class Reaction:
    name: str
    count: int
    users: list[str]


@dataclass
class Message:
    channel_id: str
    channel_name: str
    ts: datetime.datetime
    user: str
    text: str
    thread_ts: Optional[datetime.datetime] = None
    reply_count: int = 0
    reactions: list[Reaction] = field(default_factory=list)

@dataclass
class Channel:
    channel_id: str
    channel_name: str

def write_csv(data, filename):
    with open(filename, mode="a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow(row)


def write_channel_data_for_csv(start_time, end_time, message_data):
    jst = pytz.timezone('Asia/Tokyo')
    start = jst.localize(start_time)
    end = jst.localize(end_time)
    timestr = start.strftime('%Y%m%d%H%M%S') + '-' + end.strftime('%Y%m%d%H%M%S')
    messages_csv = f"slack_messages_{timestr}.csv"
    reactions_csv = f"slack_reactions_{timestr}.csv"

    messages = [["channel_id", "channel_name", "ts", "user", "text", "thread_ts", "reply_count"]]
    reactions = [["channel_id", "channel_name", "ts", "user", "reaction_name", "reaction_count", "reaction_user"]]

    for _, m in message_data.items():
        messages.append([m.channel_id, m.channel_name, m.ts, m.user, m.text, m.thread_ts, m.reply_count])
        for r in m.reactions:
            for u in r.users:
                reactions.append([m.channel_id, m.channel_name, m.ts, m.user, r.name, r.count, u])

    write_csv(messages, messages_csv)
    write_csv(reactions, reactions_csv)

def write_channel_data_for_database(message_data, channel_list):
    messages = []
    reactions = []

    for _, m in message_data.items():
        messages.append([m.channel_id, m.ts, m.user, m.text, m.thread_ts, m.reply_count])
        for r in m.reactions:
            for u in r.users:
                reactions.append([m.channel_id, m.ts, m.user, r.name, r.count, u])

    channels = [[channel.channel_id, channel.channel_name] for channel in channel_list]

    db = Db()
    db.insert_channel_data(channels)
    db.insert_message_data(messages)
    db.insert_reaction_data(reactions)

def process_message(message, channel_id, channel_name, message_data):
    ts = datetime.datetime.fromtimestamp(float(message.get("ts", "")))
    user = message.get("user", "")
    text = message.get("text", "")
    thread_ts = message.get("thread_ts", "")
    # スレッドがない場合、thread_tsをNULLで登録する
    if thread_ts == "":
        thread_ts = None
    else:
        thread_ts = datetime.datetime.fromtimestamp(float(thread_ts))
    reply_count = message.get("reply_count", 0)

    reactions = []
    if "reactions" in message:
        for reaction in message["reactions"]:
            reaction_users = reaction.get("users", [])
            reactions.append(Reaction(reaction["name"], reaction["count"], reaction_users))

    key = (channel_id, ts)
    if key not in message_data:
        message_data[key] = Message(channel_id, channel_name, ts, user, text, thread_ts, reply_count, reactions)

class Db:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
        except psycopg2.Error as e:
            print(f"Failed to connect to database: {e}")
            raise e
        else:
            with self.conn.cursor() as cursor:
                cursor.execute("set local timezone to 'Asia/Tokyo';")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS slack_messages (
                        channel_id VARCHAR(20) NOT NULL,
                        ts TIMESTAMP NOT NULL,
                        user_id VARCHAR(20) NOT NULL,
                        text VARCHAR,
                        thread_ts TIMESTAMP,
                        reply_count INTEGER,
                        PRIMARY KEY(channel_id, ts)
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS slack_reactions (
                        channel_id VARCHAR(20) NOT NULL,
                        ts TIMESTAMP NOT NULL,
                        message_user_id VARCHAR(20) NOT NULL,
                        reaction_name VARCHAR(20) NOT NULL,
                        reaction_count INTEGER,
                        reaction_user_id VARCHAR(20),
                        PRIMARY KEY(channel_id, ts, reaction_name, reaction_user_id)
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS slack_channels (
                        channel_id VARCHAR(20) NOT NULL,
                        channel_name VARCHAR(20) NOT NULL,
                        PRIMARY KEY(channel_id)
                    );
                """)
                self.conn.commit()

    def insert_message_data(self, messages):
        if len(messages) == 0 :
            print("Skip registration because reactions have no data.")
            return

        try:
            with self.conn.cursor() as cursor:
                query = f"INSERT INTO slack_messages (channel_id, ts, user_id, text, thread_ts, reply_count) VALUES %s ON CONFLICT DO NOTHING;"
                extras.execute_values(cursor, query, messages)
                self.conn.commit()
        except psycopg2.Error as e:
            print(f"Failed to insert data: {e}")
            self.conn.rollback()
            raise e

    def insert_reaction_data(self, reactions):
        if len(reactions) == 0 :
            print("Skip registration because reactions have no data.")
            return

        try:
            with self.conn.cursor() as cursor:
                query = f"INSERT INTO slack_reactions (channel_id, ts, message_user_id, reaction_name, reaction_count, reaction_user_id) VALUES %s ON CONFLICT DO NOTHING;"
                extras.execute_values(cursor, query, reactions)
                self.conn.commit()
        except psycopg2.Error as e:
            print(f"Failed to insert data: {e}")
            self.conn.rollback()
            raise e

    def insert_channel_data(self, channels):
        if len(channels) == 0 :
            print("Skip registration because channels have no data.")
            return

        try:
            with self.conn.cursor() as cursor:
                # 新規channel又はchannel_nameが変更された場合のみ更新を行う
                query = f"INSERT INTO slack_channels (channel_id, channel_name) VALUES %s ON CONFLICT (channel_id) DO UPDATE SET channel_name = excluded.channel_name WHERE slack_channels.channel_name <> excluded.channel_name;"
                extras.execute_values(cursor, query, channels)
                self.conn.commit()
        except psycopg2.Error as e:
            print(f"Failed to insert data: {e}")
            self.conn.rollback()
            raise e

class SlackBot:
    def __init__(self):
        self.token = self.load_token()
        self.client = WebClient(token=self.token)

    @staticmethod
    def load_token():
        load_dotenv()
        slack_bot_token = os.environ.get("SLACK_BOT_TOKEN")

        if not slack_bot_token:
            print("SLACK_BOT_TOKENが設定されていません")
            exit()

        return slack_bot_token

    def get_channels(self):
        channels = []
        cursor = None
        while True:
            try:
                response = self.client.conversations_list(
                    limit=1000,
                    cursor=cursor,
                    types="public_channel"  # ",private_channel"
                )
                channels += response["channels"]
                cursor = response["response_metadata"].get("next_cursor")
                if not cursor:
                    break
            except SlackApiError as e:
                print("Error:", e)
                break
        return channels

    def get_channel_history(self, channel_id, oldest, latest, limit=1000):
        messages = []
        while True:
            try:
                response = self.client.conversations_history(
                    channel=channel_id,
                    oldest=oldest,
                    latest=latest,
                    limit=limit
                )
                messages += response["messages"]
                if not response["has_more"]:
                    break
                oldest = messages[-1]["ts"]
                time.sleep(1)  # レートリミットを考慮して1秒待機する
            except SlackApiError as e:
                if e.response["error"] == "not_in_channel":
                    # botはチャネルに入っていないとメッセージを取得できないので入る
                    response = self.client.conversations_join(channel=channel_id)
                    print("joined slack channel:", channel_id)
                    continue
                print("Error:", e)
                break
        return messages

    def process_channel(self, channel, oldest, latest, message_data):
        channel_id = channel["id"]
        channel_name = channel["name"]
        channel_history = self.get_channel_history(channel_id, oldest, latest)
        print("processing", channel_id, channel_name)

        for message in channel_history:
            process_message(message, channel_id, channel_name, message_data)

            if "thread_ts" in message:
                thread_ts = message["thread_ts"]
                try:
                    response = bot.client.conversations_replies(
                        channel=channel_id, ts=thread_ts, oldest=oldest, latest=latest, limit=1000)
                    time.sleep(1)
                except SlackApiError as e:
                    print(f"Error: {e}")
                    continue

                for reply in response["messages"]:
                    if reply.get("ts") == thread_ts:
                        continue
                    process_message(reply, channel_id, channel_name, message_data)

    def create_messages_and_reactions(self, start_time, end_time):
        start_timestamp = time.mktime(start_time.timetuple())
        end_timestamp = time.mktime(end_time.timetuple())

        channels = self.get_channels()
        message_data = {}
        channel_list = []

        for channel in channels:
            is_archived = channel["is_archived"]
            if is_archived:
                continue
            self.process_channel(channel, start_timestamp, end_timestamp, message_data)

            channel_list.append(Channel(channel["id"], channel["name"]))

        return message_data, channel_list

    def export_data_to_database(self, start_time, end_time):
        message_data, channel_list = self.create_messages_and_reactions(start_time, end_time)
        write_channel_data_for_database(message_data, channel_list)

    def export_data_to_csv(self, start_time, end_time):
        message_data, _ = self.create_messages_and_reactions(start_time, end_time)
        write_channel_data_for_csv(start_time, end_time, message_data)


# Message data sample
# cf. https://api.slack.com/events/message#stars__pins__and_reactions
"""
{
       "type": "message",
       "channel": "C2147483705",
       "user": "U2147483697",
       "text": "Hello world",
       "ts": "1355517523.000005",
       "is_starred": true,
       "pinned_to": ["C024BE7LT", ...],
       "reactions": [
               {
                       "name": "astonished",
                       "count": 3,
                       "users": [ "U1", "U2", "U3" ]
               },
               {
                       "name": "facepalm",
                       "count": 5,
                       "users": [ "U1", "U2", "U3", "U4", "U5" ]
               }
       ]
}
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export data from SlackBot.")
    parser.add_argument("output_type", choices=["csv", "db"], help="Output type: 'csv' or 'db'")
    args = parser.parse_args()

    start_time = datetime.datetime(2000, 1, 1)
    # start_time = datetime.datetime(2023, 5, 1)
    end_time = datetime.datetime.now()

    bot = SlackBot()
    output_type = args.output_type
    if output_type == "csv":
        bot.export_data_to_csv(start_time, end_time)
    elif output_type == "db":
        bot.export_data_to_database(start_time, end_time)
    else:
        parser.error("Invalid output type. Please specify 'csv' or 'db'.")
