USE exchange_domain;

/*
  目的:
  - 法定または自社ルールの確認閾値をわずかに下回る売買を、短時間に分散している候補を抽出する。
  - 金融庁参考事例の「取引時確認の対象となる金額をわずかに下回るように分散」に対応する一次スクリーニング。

  想定シナリオ:
  - 同一ユーザー・同一通貨ペアで、1件あたり 1,000,000 未満の約定が24時間に3件以上ある。
  - 合計額は閾値を超えているが、各約定は閾値直下に寄っている。

  注意点:
  - 閾値はサンプルとして 1,000,000 を固定している。
  - 実務では法定閾値や社内閾値をアプリ側パラメータで差し込むのがよい。
*/
WITH threshold_trades AS (
  SELECT
    te.user_id,
    te.from_currency_id,
    te.to_currency_id,
    te.from_amount,
    te.executed_at
  FROM trade_executions te
  WHERE
    te.from_amount >= 900000
    AND te.from_amount < 1000000
)
SELECT
  u.id AS user_id,
  u.member_code,
  fc.code AS from_currency_code,
  tc.code AS to_currency_code,
  COUNT(*) AS trade_count,
  SUM(tt.from_amount) AS total_from_amount,
  ROUND(AVG(tt.from_amount), 18) AS avg_from_amount,
  MIN(tt.from_amount) AS min_from_amount,
  MAX(tt.from_amount) AS max_from_amount,
  MIN(tt.executed_at) AS first_executed_at,
  MAX(tt.executed_at) AS last_executed_at,
  ROUND((1000000 - MAX(tt.from_amount)) / 1000000 * 100, 4) AS threshold_gap_percent
FROM threshold_trades tt
INNER JOIN users u ON u.id = tt.user_id
INNER JOIN currencies fc ON fc.id = tt.from_currency_id
INNER JOIN currencies tc ON tc.id = tt.to_currency_id
WHERE tt.executed_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 24 HOUR)
GROUP BY
  u.id,
  u.member_code,
  fc.code,
  tc.code
HAVING
  COUNT(*) >= 3
  AND SUM(tt.from_amount) >= 1000000
ORDER BY
  total_from_amount DESC,
  trade_count DESC,
  last_executed_at DESC,
  user_id;
