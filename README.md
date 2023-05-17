# Slack Exporter

ボットが存在しているチャネルのSlackのデータを取得し、CSV形式でダウンロード又はDBへの書き込みを実行できます。

## 使い方

以下の手順に従い、実行します。

### SlackのAPIトークンを取得する

以下のブログがわかりやすいです。
<https://risaki-masa.com/how-to-get-api-token-in-slack/>

この際、`App Name`に指定した名前がボットの名前になるので、良い名前をつけてあげてください。

許可をするべきscopeは、以下の三つです。

- channels:history
- channels:join
- channels:read

### `.env`ファイルを作り、SLACK_BOT_TOKENとDATABASE_URLを指定します。

取得できたAPIトークンを`SLACK_BOT_TOKEN`という環境変数名にセットし、DB接続用のURLを`DATABASE_URL`にセットします。(csv出力のみの場合`DATABASE_URL`は不要です)
`.env`というファイルを作り中に入れておくと簡単です。
普通に環境変数に設定したのでも大丈夫です。

```shell
SLACK_BOT_TOKEN=xoxb-....
DATABASE_URL=postgres://{user}:{password}@{host}/{db_name}
```

### Pythonプログラムを実行

何らかの方法でpython3を実行できるようにしてください。

その後は、一度だけ以下を実行します。

```shell
poetry install
```

取得したいたびに以下を実行するとSlackの中身を取得することができます。

- csvとして出力したい場合

```shell
poetry run python main.py csv
```

- DBへの書き込みを行う場合

```shell
poetry run python main.py db
```
