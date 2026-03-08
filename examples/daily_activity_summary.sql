USE exchange_domain;

/*
  目的:
  - 日次の主要アクティビティ件数を1行に集約して、業務ボリュームを俯瞰する。
  - 取引(注文/約定)と入出金(法定通貨/暗号資産)の偏りを同じ軸で比較する。

  設計意図:
  - テーブルごとに時刻カラム名が異なるため、まず UNION ALL で「日付+件数」へ正規化。
  - その後、外側クエリで activity_date 単位に SUM して横持ちの集計表を作る。
  - UNION ではなく UNION ALL を使い、重複排除コストを避ける。

  出力の読み方:
  - trading_order_count と trade_execution_count の差が大きい日は、未約定注文が多い可能性。
  - deposits/withdrawals の偏りで資金流入超過・流出超過の傾向を確認できる。
*/
SELECT
  activity_date,
  SUM(trading_order_count) AS trading_order_count,
  SUM(trade_execution_count) AS trade_execution_count,
  SUM(fiat_deposit_count) AS fiat_deposit_count,
  SUM(fiat_withdrawal_count) AS fiat_withdrawal_count,
  SUM(crypto_deposit_count) AS crypto_deposit_count,
  SUM(crypto_withdrawal_count) AS crypto_withdrawal_count
FROM (
  SELECT
    DATE(placed_at) AS activity_date,
    COUNT(*) AS trading_order_count,
    0 AS trade_execution_count,
    0 AS fiat_deposit_count,
    0 AS fiat_withdrawal_count,
    0 AS crypto_deposit_count,
    0 AS crypto_withdrawal_count
  FROM trading_orders
  GROUP BY DATE(placed_at)

  UNION ALL

  SELECT
    DATE(executed_at) AS activity_date,
    0 AS trading_order_count,
    COUNT(*) AS trade_execution_count,
    0 AS fiat_deposit_count,
    0 AS fiat_withdrawal_count,
    0 AS crypto_deposit_count,
    0 AS crypto_withdrawal_count
  FROM trade_executions
  GROUP BY DATE(executed_at)

  UNION ALL

  SELECT
    DATE(requested_at) AS activity_date,
    0 AS trading_order_count,
    0 AS trade_execution_count,
    COUNT(*) AS fiat_deposit_count,
    0 AS fiat_withdrawal_count,
    0 AS crypto_deposit_count,
    0 AS crypto_withdrawal_count
  FROM fiat_deposits
  GROUP BY DATE(requested_at)

  UNION ALL

  SELECT
    DATE(requested_at) AS activity_date,
    0 AS trading_order_count,
    0 AS trade_execution_count,
    0 AS fiat_deposit_count,
    COUNT(*) AS fiat_withdrawal_count,
    0 AS crypto_deposit_count,
    0 AS crypto_withdrawal_count
  FROM fiat_withdrawals
  GROUP BY DATE(requested_at)

  UNION ALL

  SELECT
    DATE(detected_at) AS activity_date,
    0 AS trading_order_count,
    0 AS trade_execution_count,
    0 AS fiat_deposit_count,
    0 AS fiat_withdrawal_count,
    COUNT(*) AS crypto_deposit_count,
    0 AS crypto_withdrawal_count
  FROM crypto_deposits
  GROUP BY DATE(detected_at)

  UNION ALL

  SELECT
    DATE(requested_at) AS activity_date,
    0 AS trading_order_count,
    0 AS trade_execution_count,
    0 AS fiat_deposit_count,
    0 AS fiat_withdrawal_count,
    0 AS crypto_deposit_count,
    COUNT(*) AS crypto_withdrawal_count
  FROM crypto_withdrawals
  GROUP BY DATE(requested_at)
) daily
GROUP BY activity_date
ORDER BY activity_date;
