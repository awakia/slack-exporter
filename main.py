## this bot needs permissions below:
# channels:history,channels:join,channels:read

import os
import csv
import datetime
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# 環境変数からSLACK_BOT_TOKENを取得する
SLACK_BOT_TOKEN = None

def load_token():
    load_dotenv()
    # 環境変数からSLACK_BOT_TOKENを取得する
    global SLACK_BOT_TOKEN
    SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")

    # SLACK_BOT_TOKENが設定されていない場合はエラーを出力して終了する
    if not SLACK_BOT_TOKEN:
        print("SLACK_BOT_TOKENが設定されていません")
        exit()

# チャネル一覧を取得する関数
def get_channels(client):
    channels = []
    cursor = None
    while True:
        try:
            response = client.conversations_list(
                limit=1000,
                cursor=cursor,
                types="public_channel" # ",private_channel"
            )
            channels += response["channels"]
            cursor = response["response_metadata"].get("next_cursor")
            if not cursor:
                break
        except SlackApiError as e:
            print("Error:", e)
            break
    return channels

# 発言一覧を取得する関数
def get_channel_history(client, channel_id, limit=1000):
    messages = []
    oldest = 0
    while True:
        try:
            response = client.conversations_history(
                channel=channel_id,
                limit=limit,
                oldest=oldest
            )
            messages += response["messages"]
            if not response["has_more"]:
                break
            oldest = messages[-1]["ts"]
        except SlackApiError as e:
            if e.response["error"] == "not_in_channel":
                # botはチャネルに入っていないとメッセージを取得できないので入る
                response = client.conversations_join(channel=channel_id)
                print("joined slack channel:", channel_id)
                continue
            print("Error:", e)
            break
    return messages

# CSVファイルに書き出す関数
def write_csv(data, filename):
    with open(filename, mode="wa", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow(row)


## Message data sample
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
			"count": 1034,
			"users": [ "U1", "U2", "U3", "U4", "U5" ]
		}
	]
}
"""

def main():
    load_token()

    t_delta = datetime.timedelta(hours=9)
    jst = datetime.timezone(t_delta, 'JST')
    now = datetime.datetime.now(jst)
    timestr = now.strftime('%Y%m%d%H%M%S')

    client = WebClient(token=SLACK_BOT_TOKEN)
    # チャネルと発言の一覧を取得する
    channels = get_channels(client)
    messages = [["channel_id", "channel_name", "ts", "user", "text", "thread_ts", "reply_count"]]
    reactions = [["channel_id", "channel_name", "ts", "user", "reaction_name", "reaction_count", "reaction_user"]]
    replies = [["channel_id", "channel_name", "ts", "user", "text", "thread_ts", "reply_count"]]

    # CSVファイルを用意
    messages_csv = f"slack_messages_{timestr}.csv"
    reactions_csv = f"slack_reactions_{timestr}.csv"
    replies_csv = f"slack_replies_{timestr}.csv"
    write_csv(messages, messages_csv)
    write_csv(reactions, reactions_csv)
    write_csv(replies, replies_csv)

    for channel in channels:
        is_archived = channel["is_archived"]
        if is_archived:
            continue
        channel_id = channel["id"]
        channel_name = channel["name"]
        channel_history = get_channel_history(client, channel_id)
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
                    # cf. https://api.slack-gov.com/methods/conversations.replies

                    # NOTE: 本来はpaginationしないといけないが、1000を超えることはないと信じて手抜きする。
                    response = client.conversations_replies(channel=channel_id, ts=thread_ts, limit=1000)
                except SlackApiError as e:
                    print(f"Error: {e}")
                    continue

                for reply in response["messages"]:
                    ts = reply.get("ts", "")
                    user = reply.get("user", "")
                    text = reply.get("text", "")
                    thread_ts = reply.get("thread_ts", "")
                    replies.append([channel_id, channel_name, ts, user, text, thread_ts, ""])

                    if "reactions" in reply:
                        for reaction in reply["reactions"]:
                            users = ",".join(reaction.get("users", []))
                            reactions.append([channel_id, channel_name, ts, reaction["name"], reaction["count"], users])

        # csvに1チャネル文を書き出す
        write_csv(messages, messages_csv)
        write_csv(reactions, reactions_csv)
        write_csv(replies, replies_csv)
        messages = []
        reactions = []
        replies = []

if __name__ == "__main__":
    main()
