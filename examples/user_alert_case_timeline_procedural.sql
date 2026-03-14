USE exchange_domain;

/*
  コピー元:
  - examples/user_alert_case_timeline.sql

  元SQLの問題点:
  - イベント正規化と画面向けの並び順付けが同居すると、
    タイムライン表示ルールを変えた時に SQL 全体を触る範囲が大きい。
  - 実務では sequence_no や lifecycle_stage のような補助列を後から足したくなるので、
    一段階ずつ処理した方が変更しやすい。

  この手続き版でしていること:
  1. 異種イベントを一時表へ正規化する。
  2. ユーザー内の時系列番号を付ける。
  3. 画面やレビューで使いやすい lifecycle_stage を後付けする。
*/
DROP TEMPORARY TABLE IF EXISTS tmp_timeline_events;
CREATE TEMPORARY TABLE tmp_timeline_events AS
SELECT
  ael.user_id,
  ael.detected_at AS event_at,
  'ALERT_DETECTED' AS event_type,
  ael.id AS event_id,
  ar.rule_name AS primary_label,
  CONCAT('score=', ael.score, ', status=', aes.value) AS secondary_label
FROM alert_event_logs ael
INNER JOIN alert_rules ar ON ar.id = ael.rule_id
INNER JOIN alert_event_statuses aes ON aes.id = ael.alert_event_status_id

UNION ALL

SELECT
  sc.user_id,
  sc.opened_at AS event_at,
  'CASE_OPENED' AS event_type,
  sc.id AS event_id,
  sc.title AS primary_label,
  CONCAT('status=', cs.value, ', risk=', rl.value) AS secondary_label
FROM suspicious_cases sc
INNER JOIN case_statuses cs ON cs.id = sc.current_status_id
INNER JOIN risk_levels rl ON rl.id = sc.risk_level_id

UNION ALL

SELECT
  aa.user_id,
  aa.requested_at AS event_at,
  'ACCOUNT_ACTION' AS event_type,
  aa.id AS event_id,
  aat.value AS primary_label,
  aa.action_reason AS secondary_label
FROM account_actions aa
INNER JOIN account_action_types aat ON aat.id = aa.action_type_id

UNION ALL

SELECT
  usce.user_id,
  usce.occurred_at AS event_at,
  'USER_STATUS_CHANGED' AS event_type,
  usce.id AS event_id,
  uset.value AS primary_label,
  usce.reason AS secondary_label
FROM user_status_change_events usce
INNER JOIN user_status_event_types uset ON uset.id = usce.event_type_id;

DROP TEMPORARY TABLE IF EXISTS tmp_timeline_ranked;
CREATE TEMPORARY TABLE tmp_timeline_ranked AS
SELECT
  te.user_id,
  te.event_at,
  te.event_type,
  te.event_id,
  te.primary_label,
  te.secondary_label,
  ROW_NUMBER() OVER (
    PARTITION BY te.user_id
    ORDER BY te.event_at ASC, te.event_type ASC, te.event_id ASC
  ) AS event_sequence_no,
  CASE te.event_type
    WHEN 'ALERT_DETECTED' THEN 'DETECTION'
    WHEN 'CASE_OPENED' THEN 'CASE_MANAGEMENT'
    WHEN 'ACCOUNT_ACTION' THEN 'ACCOUNT_CONTROL'
    WHEN 'USER_STATUS_CHANGED' THEN 'STATUS_CONTROL'
    ELSE 'OTHER'
  END AS lifecycle_stage
FROM tmp_timeline_events te;

-- RESULT_QUERY
SELECT
  u.id AS user_id,
  u.member_code,
  tr.event_at,
  tr.event_type,
  tr.event_id,
  tr.primary_label,
  tr.secondary_label,
  tr.event_sequence_no,
  tr.lifecycle_stage
FROM tmp_timeline_ranked tr
INNER JOIN users u ON u.id = tr.user_id
ORDER BY tr.event_at DESC, u.id, tr.event_type, tr.event_id;
