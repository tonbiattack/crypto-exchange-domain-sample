USE exchange_domain;

/*
  コピー元:
  - examples/suspicious_rapid_outflow_candidates.sql

  元SQLの問題点:
  - 流入抽出、出金照合、閾値判定、優先度付けを 1 本の SELECT に詰め込むと、
    二次審査用の補助列を足したい時に見通しが落ちやすい。
  - 「候補抽出」と「レビュー順の整形」は別段階にした方が、閾値変更や理由文追加に強い。

  この手続き版でしていること:
  1. 完了済み流入を一時表へ退避する。
  2. 完了済み出金を一時表へ退避する。
  3. 24時間内の出金合計を別表に集約する。
  4. 閾値判定と review_priority / review_reason を後段で付与する。
*/
SET @rapid_outflow_window_hours := 24;
SET @rapid_outflow_ratio_threshold := 0.80;

DROP TEMPORARY TABLE IF EXISTS tmp_rapid_inflows;
CREATE TEMPORARY TABLE tmp_rapid_inflows AS
SELECT
  fd.id AS inflow_id,
  'FIAT_DEPOSIT' AS inflow_type,
  fd.user_id,
  fd.currency_id,
  fd.amount AS inflow_amount,
  fd.completed_at AS inflow_completed_at
FROM fiat_deposits fd
INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
WHERE ds.value = 'COMPLETED'
  AND fd.completed_at IS NOT NULL

UNION ALL

SELECT
  cd.id AS inflow_id,
  'CRYPTO_DEPOSIT' AS inflow_type,
  cd.user_id,
  cd.currency_id,
  cd.amount AS inflow_amount,
  cd.confirmed_at AS inflow_completed_at
FROM crypto_deposits cd
INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
WHERE ds.value = 'COMPLETED'
  AND cd.confirmed_at IS NOT NULL;

DROP TEMPORARY TABLE IF EXISTS tmp_rapid_outflows;
CREATE TEMPORARY TABLE tmp_rapid_outflows AS
SELECT
  fw.id AS outflow_id,
  'FIAT_WITHDRAWAL' AS outflow_type,
  fw.user_id,
  fw.currency_id,
  fw.amount AS outflow_amount,
  fw.completed_at AS outflow_completed_at
FROM fiat_withdrawals fw
INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
WHERE ws.value = 'COMPLETED'
  AND fw.completed_at IS NOT NULL

UNION ALL

SELECT
  cw.id AS outflow_id,
  'CRYPTO_WITHDRAWAL' AS outflow_type,
  cw.user_id,
  cw.currency_id,
  cw.amount AS outflow_amount,
  cw.completed_at AS outflow_completed_at
FROM crypto_withdrawals cw
INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
WHERE ws.value = 'COMPLETED'
  AND cw.completed_at IS NOT NULL;

DROP TEMPORARY TABLE IF EXISTS tmp_rapid_outflow_rollups;
CREATE TEMPORARY TABLE tmp_rapid_outflow_rollups AS
SELECT
  i.user_id,
  i.currency_id,
  i.inflow_type,
  i.inflow_id,
  i.inflow_completed_at,
  i.inflow_amount,
  COUNT(o.outflow_id) AS matched_outflow_count,
  COALESCE(SUM(o.outflow_amount), 0) AS matched_outflow_amount,
  ROUND(COALESCE(SUM(o.outflow_amount), 0) / NULLIF(i.inflow_amount, 0), 4) AS outflow_ratio,
  CASE
    WHEN COALESCE(SUM(o.outflow_amount), 0) / NULLIF(i.inflow_amount, 0) >= 0.95 THEN 'CRITICAL'
    WHEN COALESCE(SUM(o.outflow_amount), 0) / NULLIF(i.inflow_amount, 0) >= 0.80 THEN 'HIGH'
    ELSE 'REVIEW'
  END AS review_priority,
  CASE
    WHEN COUNT(o.outflow_id) = 0 THEN '24時間以内の出金なし'
    WHEN COALESCE(SUM(o.outflow_amount), 0) / NULLIF(i.inflow_amount, 0) >= 0.95 THEN '入金の95%以上が24時間以内に流出'
    ELSE '入金の80%以上が24時間以内に流出'
  END AS review_reason
FROM tmp_rapid_inflows i
LEFT JOIN tmp_rapid_outflows o
  ON o.user_id = i.user_id
  AND o.currency_id = i.currency_id
  AND o.outflow_completed_at >= i.inflow_completed_at
  AND o.outflow_completed_at < DATE_ADD(i.inflow_completed_at, INTERVAL @rapid_outflow_window_hours HOUR)
GROUP BY
  i.user_id,
  i.currency_id,
  i.inflow_type,
  i.inflow_id,
  i.inflow_completed_at,
  i.inflow_amount
HAVING matched_outflow_count > 0
   AND matched_outflow_amount >= i.inflow_amount * @rapid_outflow_ratio_threshold;

-- RESULT_QUERY
SELECT
  u.id AS user_id,
  u.member_code,
  c.code AS currency_code,
  r.inflow_type,
  r.inflow_id,
  r.inflow_completed_at,
  r.inflow_amount,
  r.matched_outflow_count,
  r.matched_outflow_amount,
  r.outflow_ratio,
  r.review_priority,
  r.review_reason
FROM tmp_rapid_outflow_rollups r
INNER JOIN users u ON u.id = r.user_id
INNER JOIN currencies c ON c.id = r.currency_id
ORDER BY
  CASE r.review_priority
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    ELSE 3
  END,
  r.outflow_ratio DESC,
  r.matched_outflow_amount DESC,
  r.inflow_completed_at DESC;
