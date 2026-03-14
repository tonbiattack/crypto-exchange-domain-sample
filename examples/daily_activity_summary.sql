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
  -- 集計軸の日付。
  activity_date,
  -- 当日に受け付けた注文件数。
  SUM(trading_order_count) AS trading_order_count,
  -- 当日に成立した約定件数。
  SUM(trade_execution_count) AS trade_execution_count,
  -- 法定通貨入金申請件数。
  SUM(fiat_deposit_count) AS fiat_deposit_count,
  -- 法定通貨出金申請件数。
  SUM(fiat_withdrawal_count) AS fiat_withdrawal_count,
  -- 暗号資産入金検知件数。
  SUM(crypto_deposit_count) AS crypto_deposit_count,
  -- 暗号資産出金申請件数。
  SUM(crypto_withdrawal_count) AS crypto_withdrawal_count
FROM (
  SELECT
    -- 注文受付日を業務日として扱う。
    DATE(placed_at) AS activity_date,
    -- 注文系だけ件数を立てる。
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
    -- 約定成立日を業務日として扱う。
    DATE(executed_at) AS activity_date,
    0 AS trading_order_count,
    -- 約定系だけ件数を立てる。
    COUNT(*) AS trade_execution_count,
    0 AS fiat_deposit_count,
    0 AS fiat_withdrawal_count,
    0 AS crypto_deposit_count,
    0 AS crypto_withdrawal_count
  FROM trade_executions
  GROUP BY DATE(executed_at)

  UNION ALL

  SELECT
    -- 法定入金は requested_at ベースで日次集計。
    DATE(requested_at) AS activity_date,
    0 AS trading_order_count,
    0 AS trade_execution_count,
    -- 法定入金件数。
    COUNT(*) AS fiat_deposit_count,
    0 AS fiat_withdrawal_count,
    0 AS crypto_deposit_count,
    0 AS crypto_withdrawal_count
  FROM fiat_deposits
  GROUP BY DATE(requested_at)

  UNION ALL

  SELECT
    -- 法定出金も requested_at ベースでそろえる。
    DATE(requested_at) AS activity_date,
    0 AS trading_order_count,
    0 AS trade_execution_count,
    0 AS fiat_deposit_count,
    -- 法定出金件数。
    COUNT(*) AS fiat_withdrawal_count,
    0 AS crypto_deposit_count,
    0 AS crypto_withdrawal_count
  FROM fiat_withdrawals
  GROUP BY DATE(requested_at)

  UNION ALL

  SELECT
    -- 暗号資産入金は検知時刻 detected_at を利用。
    DATE(detected_at) AS activity_date,
    0 AS trading_order_count,
    0 AS trade_execution_count,
    0 AS fiat_deposit_count,
    0 AS fiat_withdrawal_count,
    -- 暗号資産入金件数。
    COUNT(*) AS crypto_deposit_count,
    0 AS crypto_withdrawal_count
  FROM crypto_deposits
  GROUP BY DATE(detected_at)

  UNION ALL

  SELECT
    -- 暗号資産出金は申請時刻 requested_at を利用。
    DATE(requested_at) AS activity_date,
    0 AS trading_order_count,
    0 AS trade_execution_count,
    0 AS fiat_deposit_count,
    0 AS fiat_withdrawal_count,
    0 AS crypto_deposit_count,
    -- 暗号資産出金件数。
    COUNT(*) AS crypto_withdrawal_count
  FROM crypto_withdrawals
  GROUP BY DATE(requested_at)
) daily
-- 日付単位で6種類の業務件数を横持ちに集約する。
GROUP BY activity_date
-- 時系列で確認しやすいよう昇順。
ORDER BY activity_date;
