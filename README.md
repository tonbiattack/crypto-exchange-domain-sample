# private-crypto-exchange-domain-sample

MySQL のテーブル定義と、Docker での起動設定を含むサンプルです。

## 起動方法

```bash
docker compose up -d
```

### 初回起動時に自動実行されるSQL

`docker/mysql/init` 配下の `.sql` は、MySQLコンテナの**初回起動時のみ**自動実行されます。
（実行順はファイル名順）

1. `docker/mysql/init/01_tables.sql`（DB・テーブル作成）
2. `docker/mysql/init/02_seed.sql`（区分値の初期データ）
3. `docker/mysql/init/03_sample_transactions.sql`（取引/入出金サンプルデータ）

`01_tables.sql` にはテーブル/カラムコメント定義も含まれます。

注意:
- 既存の `mysql_data` ボリュームがある場合、上記SQLは自動再実行されません。
- 再適用したい場合は、以下の手動実行コマンドを利用してください。

既存ボリュームを使っていてシードを再投入したい場合:

```bash
docker exec private-crypto-exchange-mysql sh -c "mysql -uapp -papp < /docker-entrypoint-initdb.d/02_seed.sql"
```

取引系サンプルデータを投入したい場合:

```bash
docker exec private-crypto-exchange-mysql sh -c "mysql -uapp -papp < /docker-entrypoint-initdb.d/03_sample_transactions.sql"
```

## 集計SQLサンプル

- `examples/daily_activity_summary.sql`
  - 日次の取引・入出金件数を横並びで確認
- `examples/pair_volume_summary.sql`
  - 通貨ペア別の出来高（件数・数量・約定金額）を確認
- `examples/failure_rate_summary.sql`
  - 日次の業務別失敗率（法定/暗号資産の入出金）を確認
- `examples/user_trading_ranking.sql`
  - ユーザー別の取引ランキング（約定件数・約定金額）を確認

実行例:

```bash
# PowerShell
Get-Content -Raw examples/daily_activity_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/pair_volume_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/failure_rate_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/user_trading_ranking.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
```

## 接続情報

- Host: `127.0.0.1`
- Port: `3306`
- Database: `exchange_domain`
- User: `app`
- Password: `app`
- Root Password: `root`

## 停止

```bash
docker compose down
```
