USE exchange_domain;

/*
  目的:
  - 少額の売買が正常完了した直後に、高額売買が連続するパターンを抽出する。
  - 金融庁参考事例の「少額取引が正常に完了した後、すぐに高額取引が連続」に対応する一次スクリーニング。

  想定シナリオ:
  - 起点となる少額約定は 10,000 以下。
  - その後24時間以内に、1,000,000 以上の約定が2件以上発生する。

  注意点:
  - 約定ベースの抽出であり、本人属性や原資の妥当性は別途確認が必要。
*/
WITH small_trades AS (
  SELECT
    te.id,
    te.user_id,
    te.from_currency_id,
    te.to_currency_id,
    te.from_amount,
    te.executed_at
  FROM trade_executions te
  WHERE te.from_amount <= 10000
),
large_trade_links AS (
  SELECT
    st.id AS initial_trade_id,
    st.user_id,
    st.from_currency_id,
    st.to_currency_id,
    st.from_amount AS initial_from_amount,
    st.executed_at AS initial_executed_at,
    lt.id AS large_trade_id,
    lt.from_amount AS large_from_amount,
    lt.executed_at AS large_executed_at
  FROM small_trades st
  INNER JOIN trade_executions lt
    ON lt.user_id = st.user_id
    AND lt.from_currency_id = st.from_currency_id
    AND lt.to_currency_id = st.to_currency_id
    AND lt.executed_at > st.executed_at
    AND lt.executed_at < DATE_ADD(st.executed_at, INTERVAL 24 HOUR)
    AND lt.from_amount >= 1000000
)
SELECT
  u.id AS user_id,
  u.member_code,
  fc.code AS from_currency_code,
  tc.code AS to_currency_code,
  ltl.initial_executed_at,
  ltl.initial_from_amount,
  COUNT(ltl.large_trade_id) AS large_trade_count,
  SUM(ltl.large_from_amount) AS large_trade_total_amount,
  MIN(ltl.large_executed_at) AS first_large_executed_at,
  MAX(ltl.large_executed_at) AS last_large_executed_at,
  ROUND(TIMESTAMPDIFF(MINUTE, ltl.initial_executed_at, MAX(ltl.large_executed_at)) / 60, 2) AS burst_hours
FROM large_trade_links ltl
INNER JOIN users u ON u.id = ltl.user_id
INNER JOIN currencies fc ON fc.id = ltl.from_currency_id
INNER JOIN currencies tc ON tc.id = ltl.to_currency_id
GROUP BY
  u.id,
  u.member_code,
  fc.code,
  tc.code,
  ltl.initial_trade_id,
  ltl.initial_executed_at,
  ltl.initial_from_amount
HAVING COUNT(ltl.large_trade_id) >= 2
ORDER BY
  large_trade_total_amount DESC,
  burst_hours ASC,
  ltl.initial_executed_at DESC;
