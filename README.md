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
- `examples/user_net_flow_summary.sql`
  - ユーザー別・通貨別の純流入/純流出を確認
- `examples/alert_rule_detection_summary.sql`
  - ルール別の検知件数、ケース化率、平均スコアを確認
- `examples/case_backlog_summary.sql`
  - ケースの滞留件数と平均滞留日数を確認
- `examples/suspicious_withdrawal_concentration_candidates.sql`
  - 直近24時間で出金が集中したユーザー候補を確認
- `examples/user_status_timeline.sql`
  - ユーザー状態遷移の時系列監査を確認
- `examples/open_orders_staleness_summary.sql`
  - OPEN注文の滞留状況を確認
- `examples/large_failed_transactions.sql`
  - 高額な失敗取引を横断確認
- `examples/account_action_summary.sql`
  - 口座措置の実行件数を確認
- `examples/case_lead_time_summary.sql`
  - ケース対応のリードタイムを確認
- `examples/rule_false_positive_proxy.sql`
  - 誤検知が多そうなルールを proxy 指標で確認
- `examples/user_profile_change_timeline.sql`
  - プロフィール変更履歴の監査を確認
- `examples/deposit_withdrawal_lead_time_summary.sql`
  - 入出金完了までの所要時間を確認
- `examples/currency_flow_daily_summary.sql`
  - 通貨別の日次純流入を確認
- `examples/user_case_overview.sql`
  - ユーザー別の検知/ケース/措置の概要を確認
- `examples/alert_to_case_conversion_time.sql`
  - 検知からケース化までの時間を確認
- `examples/destination_address_reuse_candidates.sql`
  - 出金先アドレス使い回し候補を確認
- `examples/user_balance_reconciliation_gap.sql`
  - 入出金と約定から再計算した理論残高増減を確認
- `examples/stuck_pending_transactions.sql`
  - 長時間 PENDING の入出金を確認
- `examples/high_risk_user_activity_summary.sql`
  - 高リスクユーザーの活動量を横断確認
- `examples/alert_repeat_user_summary.sql`
  - 同一ユーザーへの繰り返し検知を確認
- `examples/status_change_after_alert.sql`
  - アラート後のステータス変更を確認
- `examples/large_unmatched_crypto_inflow.sql`
  - 高額暗号資産入金後の未売却・未出金候補を確認
- `examples/user_alert_case_timeline.sql`
  - ユーザー単位の検知/ケース/措置/状態変更タイムラインを確認
- `examples/pending_transaction_backlog_summary.sql`
  - PENDING 入出金の滞留状況を集計で確認

実行例:

```bash
# PowerShell
Get-Content -Raw examples/daily_activity_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/pair_volume_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/failure_rate_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/user_trading_ranking.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/user_net_flow_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/alert_rule_detection_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/case_backlog_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/suspicious_withdrawal_concentration_candidates.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/user_status_timeline.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/open_orders_staleness_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/large_failed_transactions.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/account_action_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/case_lead_time_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/rule_false_positive_proxy.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/user_profile_change_timeline.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/deposit_withdrawal_lead_time_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/currency_flow_daily_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/user_case_overview.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/alert_to_case_conversion_time.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/destination_address_reuse_candidates.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/user_balance_reconciliation_gap.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/stuck_pending_transactions.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/high_risk_user_activity_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/alert_repeat_user_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/status_change_after_alert.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/large_unmatched_crypto_inflow.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/user_alert_case_timeline.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
Get-Content -Raw examples/pending_transaction_backlog_summary.sql | docker exec -i private-crypto-exchange-mysql mysql -uapp -papp
```

## Go 統合テスト

このリポジトリでは、`examples/*.sql` の継続検証用に Go の実DB統合テストを追加しています。

- テストは MySQL 実DBに接続します
- 各テストはトランザクション rollback 前提で動くため、DB を汚しません
- 既定の接続先は `127.0.0.1:33306` です
- 別の接続先を使う場合は `TEST_DB_DSN` を指定します

実行例:

```bash
go test ./... -v
```

`TEST_DB_DSN` を明示する例:

```bash
# PowerShell
$env:TEST_DB_DSN="app:app@tcp(127.0.0.1:33306)/exchange_domain?parseTime=true&multiStatements=true"
go test ./... -v
```

## 接続情報

- Host: `127.0.0.1`
- Port: `33306`
- Database: `exchange_domain`
- User: `app`
- Password: `app`
- Root Password: `root`

## 停止

```bash
docker compose down
```
