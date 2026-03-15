USE exchange_domain;

/*
  コピー元:
  - examples/suspicious_transactions/large_unmatched_crypto_inflow.sql

  元SQLの問題点:
  - 大口入金抽出、売却照合、出金照合、レビュー優先度付けを 1 本でやると、
    監視条件を追加した時に相関サブクエリが膨らみやすい。
  - 候補抽出と優先度付けを分けた方が、二次審査の基準変更に追随しやすい。

  この手続き版でしていること:
  1. 高額入金だけを先に抽出する。
  2. 売却件数と出金件数を別表で集約する。
  3. 未対応候補へ review_priority / review_reason を付与する。
*/
SET @large_inflow_threshold := 1.0;
SET @large_inflow_review_days := 7;

DROP TEMPORARY TABLE IF EXISTS tmp_large_crypto_deposits;
CREATE TEMPORARY TABLE tmp_large_crypto_deposits AS
SELECT
  cd.id AS crypto_deposit_id,
  cd.user_id,
  cd.currency_id,
  cd.amount,
  cd.confirmed_at
FROM crypto_deposits cd
INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
WHERE ds.value = 'COMPLETED'
  AND cd.confirmed_at IS NOT NULL
  AND cd.amount >= @large_inflow_threshold;

DROP TEMPORARY TABLE IF EXISTS tmp_large_crypto_sell_matches;
CREATE TEMPORARY TABLE tmp_large_crypto_sell_matches AS
SELECT
  d.crypto_deposit_id,
  COUNT(te.id) AS matched_sell_execution_count
FROM tmp_large_crypto_deposits d
LEFT JOIN trade_executions te
  ON te.user_id = d.user_id
  AND te.from_currency_id = d.currency_id
  AND te.executed_at >= d.confirmed_at
  AND te.executed_at < DATE_ADD(d.confirmed_at, INTERVAL @large_inflow_review_days DAY)
GROUP BY d.crypto_deposit_id;

DROP TEMPORARY TABLE IF EXISTS tmp_large_crypto_withdraw_matches;
CREATE TEMPORARY TABLE tmp_large_crypto_withdraw_matches AS
SELECT
  d.crypto_deposit_id,
  COUNT(cw.id) AS matched_withdrawal_count
FROM tmp_large_crypto_deposits d
LEFT JOIN crypto_withdrawals cw
  ON cw.user_id = d.user_id
  AND cw.currency_id = d.currency_id
  AND cw.completed_at IS NOT NULL
  AND cw.completed_at >= d.confirmed_at
  AND cw.completed_at < DATE_ADD(d.confirmed_at, INTERVAL @large_inflow_review_days DAY)
LEFT JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
WHERE ws.value = 'COMPLETED' OR ws.value IS NULL
GROUP BY d.crypto_deposit_id;

DROP TEMPORARY TABLE IF EXISTS tmp_large_crypto_unmatched;
CREATE TEMPORARY TABLE tmp_large_crypto_unmatched AS
SELECT
  d.crypto_deposit_id,
  d.user_id,
  d.currency_id,
  d.amount,
  d.confirmed_at,
  COALESCE(sm.matched_sell_execution_count, 0) AS matched_sell_execution_count,
  COALESCE(wm.matched_withdrawal_count, 0) AS matched_withdrawal_count,
  CASE
    WHEN d.amount >= 5 THEN 'CRITICAL'
    WHEN d.amount >= 2 THEN 'HIGH'
    ELSE 'REVIEW'
  END AS review_priority,
  CASE
    WHEN d.amount >= 5 THEN '超大口入金後7日以内の売却・出金なし'
    ELSE '大口入金後7日以内の売却・出金なし'
  END AS review_reason
FROM tmp_large_crypto_deposits d
LEFT JOIN tmp_large_crypto_sell_matches sm ON sm.crypto_deposit_id = d.crypto_deposit_id
LEFT JOIN tmp_large_crypto_withdraw_matches wm ON wm.crypto_deposit_id = d.crypto_deposit_id
WHERE COALESCE(sm.matched_sell_execution_count, 0) = 0
  AND COALESCE(wm.matched_withdrawal_count, 0) = 0;

-- RESULT_QUERY
SELECT
  d.crypto_deposit_id,
  u.id AS user_id,
  u.member_code,
  c.code AS currency_code,
  d.amount,
  d.confirmed_at,
  d.matched_sell_execution_count,
  d.matched_withdrawal_count,
  d.review_priority,
  d.review_reason
FROM tmp_large_crypto_unmatched d
INNER JOIN users u ON u.id = d.user_id
INNER JOIN currencies c ON c.id = d.currency_id
ORDER BY
  CASE d.review_priority
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    ELSE 3
  END,
  d.amount DESC,
  d.confirmed_at DESC;
