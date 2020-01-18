# Trade-showcase

Trade-showcaseは、金融商品を売買するためのアプリケーションの「ショーケース」です。

`Trade`アプリケーションから秘匿情報を取り除いたものです。

## 売買可能な金融商品
現在は、エクスチェンジ:[bitFlyer lightning](https://lightning.bitflyer.com/) 、シンボル:FXBTCJPY です。

## 動作環境
 * Python >= 3.8.1
 * [pipenv](https://pipenv-ja.readthedocs.io/ja/translate-ja/basics.html)
 * `Pipfile`のpackagesセクションに定義されている各種パッケージ
 
## インストール

### 前提条件
 * Python >= 3.8.1 がインストールされている
 * [pipenv](https://pipenv-ja.readthedocs.io/ja/translate-ja/basics.html)がインストールされている

### Tradeのインストール
```
cd Trade-showcase
pipenv install
```

### テストスイートによるインストールの確認
```
pipenv shell
export PYTHONPATH=$(pwd); python trade/test_suite.py
```

## リポジトリの構造
### ライセンス
`LICENSE`

### ダッシュボード
`dashboard`

ビジュアライズされた売買結果です。

### ドキュメンテーション
`doc`

整備中です。

### コンテナ定義
`docker`

Docker Composeの利用を想定しています。

### ソースコード
`trade`

#### テストスイート

`trade/test_suite.py`

### 認証情報の定義方法
`.credentials` ファイルに、エクスチェンジ毎に定義します。

```
[bitflyer]
api_key = <your api key>
api_secret = <your api secret>
```

### 依存パッケージの定義

`Pipfile`および`Pipfile.lock`

## ストラテジ

`trade/strategy`以下は売買ストラテジが配置されています（ショーケースではスタブのみ存在します）

## 売買のプレイバックテスト

あらかじめ約定情報データベースを構築しておいてください（ショーケースでは動作検証用として historical/bitflyer/FXBTCJPY=BTCJPY 以下に構築済みです）。

以下はpipenv shellで実行する必要があります。

```
cd Trade-showcase

# プレイバックおよび結果tsvファイルの作成
export PYTHONPATH=$(pwd); python trade/scripts/run_playback.py --strategy random --sqlite-basedir historical/bitflyer/FXBTCJPY\=BTCJPY/sqlite\,reduced\=ohlc1min/ | tee simulation/random-doten.tsv
sed -i -E -e "/Draw down recovered/d" -e "s/^.*INFO://g" simulation/random-doten.tsv

# Jupyter Notebookによるビジュアライズ
jupyter notebook

# webブラウザで http://<ホスト名>:<ポート>/notebooks/dashboard/random-doten.ipynb を閲覧する
```

## 売買のリアルタイム実行

### 約定情報配信WebSocketプロキシサーバーの起動

```
cd Trade-showcase/docker

# 環境変数ファイルの作成
echo "SYMBOL=FXBTCJPY
WARM_UP_WINDOW=3second" > .env

# 起動
docker-compose up execution-proxy-server
```

### brokerの起動
```
export PYTHONPATH=$(pwd); python trade/scripts/run_realtime.py --strategy random-doten --size 0.01 --websocket-uri ws://localhost:8765/ --time-window 20second
```

