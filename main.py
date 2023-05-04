## this bot needs permissions below:
# channels:history,channels:join,channels:read

import os
import csv
import time
import datetime
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


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

    def get_channel_history(self, channel_id, limit=1000):
        messages = []
        oldest = 0
        while True:
            try:
                response = self.client.conversations_history(
                    channel=channel_id,
                    limit=limit,
                    oldest=oldest
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

    @staticmethod
    def write_csv(data, filename):
        with open(filename, mode="a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            for row in data:
                writer.writerow(row)

    def run(self):
        t_delta = datetime.timedelta(hours=9)
        jst = datetime.timezone(t_delta, 'JST')
        now = datetime.datetime.now(jst)
        timestr = now.strftime('%Y%m%d%H%M%S')

        # チャネルと発言の一覧を取得する
        channels = self.get_channels()
        messages = [["channel_id", "channel_name", "ts", "user", "text", "thread_ts", "reply_count"]]
        reactions = [["channel_id", "channel_name", "ts", "user", "reaction_name", "reaction_count", "reaction_user"]]

        # CSVファイルを用意
        messages_csv = f"slack_messages_{timestr}.csv"
        reactions_csv = f"slack_reactions_{timestr}.csv"
        self.write_csv(messages, messages_csv)
        self.write_csv(reactions, reactions_csv)

        for channel in channels:
            is_archived = channel["is_archived"]
            if is_archived:
                continue
            channel_id = channel["id"]
            channel_name = channel["name"]
            channel_history = self.get_channel_history(channel_id)
            print("processing", channel_id, channel_name)
            for message in channel_history:
                ts = message.get("ts", "")
                user = message.get("user", "")
                text = message.get("text", "")
                thread_ts = message.get("thread_ts", "")
                reply_count = message.get("reply_count", 0)
                messages.append([channel_id, channel_name, ts, user, text, thread_ts, reply_count])

                if "reactions" in message:
                    for reaction in message["reactions"]:
                        for reaction_user in reaction.get("users", []):
                            reactions.append([channel_id, channel_name, ts, user, reaction["name"], reaction["count"], reaction_user])

                # メッセージに対する返信を取得します
                if "thread_ts" in message:
                    thread_ts = message["thread_ts"]
                    try:
                        # スレッド内のメッセージを取得します
                        response = self.client.conversations_replies(channel=channel_id, ts=thread_ts, limit=1000)
                        time.sleep(1)  # レートリミットを考慮して1秒待機する
                    except SlackApiError as e:
                        print(f"Error: {e}")
                        continue

                    for reply in response["messages"]:
                        ts = reply.get("ts", "")
                        user = reply.get("user", "")
                        text = reply.get("text", "")
                        thread_ts = reply.get("thread_ts", "")
                        messages.append([channel_id, channel_name, ts, user, text, thread_ts, ""])

                        if "reactions" in reply:
                            for reaction in reply["reactions"]:
                                for reaction_user in reaction.get("users", []):
                                    reactions.append([channel_id, channel_name, ts, user, reaction["name"], reaction["count"], reaction_user])

            # csvに1チャネル文を書き出す
            self.write_csv(messages, messages_csv)
            self.write_csv(reactions, reactions_csv)
            messages = []
            reactions = []


if __name__ == "__main__":
    bot = SlackBot()
    bot.run()
