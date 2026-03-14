USE exchange_domain;

/*
  コピー元:
  - examples/pending_transaction_backlog_summary.sql

  元SQLの問題点:
  - PENDING 正規化、滞留分数算出、滞留帯分類、エスカレーション判断が 1 本に入ると、
    通知ルールの変更が SQL 本体へ波及しやすい。
  - 生データ抽出と集約後の業務判定を分けた方が、閾値追加時の影響範囲が狭い。

  この手続き版でしていること:
  1. PENDING 入出金を一時表に正規化する。
  2. pending_minutes と backlog_bucket を中間表で付与する。
  3. 集約後に escalation_needed / escalation_reason を付ける。
*/
DROP TEMPORARY TABLE IF EXISTS tmp_pending_transactions;
CREATE TEMPORARY TABLE tmp_pending_transactions AS
SELECT 'FIAT_DEPOSIT' AS operation_type, fd.currency_id, fd.requested_at AS started_at
FROM fiat_deposits fd
INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
WHERE ds.value = 'PENDING'

UNION ALL

SELECT 'FIAT_WITHDRAWAL' AS operation_type, fw.currency_id, fw.requested_at AS started_at
FROM fiat_withdrawals fw
INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
WHERE ws.value = 'PENDING'

UNION ALL

SELECT 'CRYPTO_DEPOSIT' AS operation_type, cd.currency_id, cd.detected_at AS started_at
FROM crypto_deposits cd
INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
WHERE ds.value = 'PENDING'

UNION ALL

SELECT 'CRYPTO_WITHDRAWAL' AS operation_type, cw.currency_id, cw.requested_at AS started_at
FROM crypto_withdrawals cw
INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
WHERE ws.value = 'PENDING';

DROP TEMPORARY TABLE IF EXISTS tmp_pending_classified;
CREATE TEMPORARY TABLE tmp_pending_classified AS
SELECT
  pt.operation_type,
  pt.currency_id,
  pt.started_at,
  TIMESTAMPDIFF(MINUTE, pt.started_at, CURRENT_TIMESTAMP) AS pending_minutes,
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, pt.started_at, CURRENT_TIMESTAMP) < 60 THEN 'UNDER_1_HOUR'
    WHEN TIMESTAMPDIFF(HOUR, pt.started_at, CURRENT_TIMESTAMP) < 24 THEN 'UNDER_24_HOURS'
    ELSE 'OVER_24_HOURS'
  END AS backlog_bucket
FROM tmp_pending_transactions pt;

DROP TEMPORARY TABLE IF EXISTS tmp_pending_aggregates;
CREATE TEMPORARY TABLE tmp_pending_aggregates AS
SELECT
  pc.operation_type,
  pc.currency_id,
  pc.backlog_bucket,
  COUNT(*) AS backlog_count,
  MIN(pc.started_at) AS oldest_started_at,
  MAX(pc.pending_minutes) AS max_pending_minutes,
  CASE
    WHEN pc.backlog_bucket = 'OVER_24_HOURS' THEN 1
    WHEN COUNT(*) >= 3 THEN 1
    ELSE 0
  END AS escalation_needed,
  CASE
    WHEN pc.backlog_bucket = 'OVER_24_HOURS' THEN '24時間超の滞留が存在'
    WHEN COUNT(*) >= 3 THEN '同一帯の滞留件数が3件以上'
    ELSE '通常監視'
  END AS escalation_reason
FROM tmp_pending_classified pc
GROUP BY pc.operation_type, pc.currency_id, pc.backlog_bucket;

-- RESULT_QUERY
SELECT
  pa.operation_type,
  c.code AS currency_code,
  pa.backlog_bucket,
  pa.backlog_count,
  pa.oldest_started_at,
  pa.max_pending_minutes,
  pa.escalation_needed,
  pa.escalation_reason
FROM tmp_pending_aggregates pa
INNER JOIN currencies c ON c.id = pa.currency_id
ORDER BY pa.operation_type, currency_code, pa.backlog_bucket;
