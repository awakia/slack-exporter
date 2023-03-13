# Slack Exporter

ボットが存在しているチャネルのSlackのデータを取得し、CSV形式でダウンロードできます。

## 使い方

以下の手順に従い、実行します。

### SlackのAPIトークンを取得する

以下のブログがわかりやすいです。
https://risaki-masa.com/how-to-get-api-token-in-slack/

この際、`App Name`に指定した名前がボットの名前になるので、良い名前をつけてあげてください。

許可をするべきscopeは、以下の三つです。

- channels:history
- channels:join
- channels:read

### `.env`ファイルを作り、SLACK_BOT_TOKENを指定します。

取得できたAPIトークンを`SLACK_BOT_TOKEN`という環境変数名にセットします。
`.env`というファイルを作り中に入れておくと簡単です。
普通に環境変数に設定したのでも大丈夫です。

```
SLACK_BOT_TOKEN=xoxb-....
```

### Pythonプログラムを実行

何らかの方法でpython3を実行できるようにしてください。

その後は、一度だけ以下を実行します。

```
poetry install
```

取得したいたびに以下を実行するとSlackの中身を取得することができます。

```
poetry run python main.py
```
