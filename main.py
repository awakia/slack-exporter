# this bot needs permissions below:
# channels:history,channels:join,channels:read

import csv
import datetime
import os
import time
import pytz

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def write_csv(data, filename):
    with open(filename, mode="a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow(row)


def write_channel_data(start_time, end_time, messages, reactions):
    jst = pytz.timezone('Asia/Tokyo')
    start = jst.localize(start_time)
    end = jst.localize(end_time)
    timestr = start.strftime('%Y%m%d%H%M%S') + '-' + end.strftime('%Y%m%d%H%M%S')
    messages_csv = f"slack_messages_{timestr}.csv"
    reactions_csv = f"slack_reactions_{timestr}.csv"

    write_csv(messages, messages_csv)
    write_csv(reactions, reactions_csv)


def process_message(message, channel_id, channel_name, messages, reactions):
    ts = message.get("ts", "")
    user = message.get("user", "")
    text = message.get("text", "")
    thread_ts = message.get("thread_ts", "")
    reply_count = message.get("reply_count", 0)
    messages.append([channel_id, channel_name, ts, user, text, thread_ts, reply_count])

    if "reactions" in message:
        for reaction in message["reactions"]:
            for reaction_user in reaction.get("users", []):
                data = [channel_id, channel_name, ts, user, reaction["name"], reaction["count"], reaction_user]
                reactions.append(data)


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

    def process_channel(self, channel, oldest, latest, messages, reactions, ):
        channel_id = channel["id"]
        channel_name = channel["name"]
        channel_history = self.get_channel_history(channel_id, oldest, latest)
        print("processing", channel_id, channel_name)

        for message in channel_history:
            process_message(message, channel_id, channel_name, messages, reactions)

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
                    process_message(reply, channel_id, channel_name, messages, reactions)

    def create_messages_and_reactions(self, start_time, end_time):
        start_timestamp = time.mktime(start_time.timetuple())
        end_timestamp = time.mktime(end_time.timetuple())

        channels = self.get_channels()
        messages = [["channel_id", "channel_name", "ts", "user", "text", "thread_ts", "reply_count"]]
        reactions = [["channel_id", "channel_name", "ts", "user", "reaction_name", "reaction_count", "reaction_user"]]

        for channel in channels:
            is_archived = channel["is_archived"]
            if is_archived:
                continue
            self.process_channel(channel, start_timestamp, end_timestamp, messages, reactions)

        return messages, reactions

    def export_data_to_csv(self, start_time, end_time):
        messages, reactions = self.create_messages_and_reactions(start_time, end_time)
        write_channel_data(start_time, end_time, messages, reactions)


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
    bot = SlackBot()

    start_time = datetime.datetime(2000, 1, 1)
    start_time = datetime.datetime(2023, 5, 1)
    end_time = datetime.datetime.now()

    bot.export_data_to_csv(start_time, end_time)
