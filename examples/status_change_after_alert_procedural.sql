USE exchange_domain;

/*
  コピー元:
  - examples/status_change_after_alert.sql

  元SQLの問題点:
  - アラートと状態変更の結合、最短応答の採用、応答速度ラベル付けが 1 本に寄ると、
    監査の観点を追加した時に読みづらくなりやすい。
  - まず候補関連を作り、その後に「最初の対応のみ採用」という段階へ分けた方が明快。

  この手続き版でしていること:
  1. アラート後72時間以内の状態変更候補を作る。
  2. alert_event_id ごとに最初の措置だけへ絞る。
  3. response_bucket を付けて運用初動の速さを明示する。
*/
DROP TEMPORARY TABLE IF EXISTS tmp_alert_status_candidates;
CREATE TEMPORARY TABLE tmp_alert_status_candidates AS
SELECT
  ael.id AS alert_event_id,
  ael.user_id,
  ael.rule_id,
  ael.detected_at,
  usce.id AS status_change_event_id,
  uset.value AS status_event_type,
  from_us.value AS from_status,
  to_us.value AS to_status,
  usce.reason,
  usce.occurred_at AS status_changed_at,
  TIMESTAMPDIFF(MINUTE, ael.detected_at, usce.occurred_at) AS delay_minutes,
  ROW_NUMBER() OVER (
    PARTITION BY ael.id
    ORDER BY usce.occurred_at ASC, usce.id ASC
  ) AS linked_event_rank
FROM alert_event_logs ael
INNER JOIN user_status_change_events usce
  ON usce.user_id = ael.user_id
  AND usce.occurred_at >= ael.detected_at
  AND usce.occurred_at <= DATE_ADD(ael.detected_at, INTERVAL 72 HOUR)
INNER JOIN user_status_event_types uset ON uset.id = usce.event_type_id
LEFT JOIN user_status_histories ush ON ush.status_change_event_id = usce.id AND ush.user_id = ael.user_id
LEFT JOIN user_statuses from_us ON from_us.id = ush.from_status_id
LEFT JOIN user_statuses to_us ON to_us.id = ush.to_status_id;

DROP TEMPORARY TABLE IF EXISTS tmp_alert_status_linked;
CREATE TEMPORARY TABLE tmp_alert_status_linked AS
SELECT
  c.alert_event_id,
  c.user_id,
  c.rule_id,
  c.detected_at,
  c.status_event_type,
  c.from_status,
  c.to_status,
  c.reason,
  c.status_changed_at,
  c.delay_minutes,
  c.linked_event_rank,
  CASE
    WHEN c.delay_minutes <= 30 THEN 'WITHIN_30_MINUTES'
    WHEN c.delay_minutes <= 240 THEN 'WITHIN_4_HOURS'
    ELSE 'OVER_4_HOURS'
  END AS response_bucket
FROM tmp_alert_status_candidates c
WHERE c.linked_event_rank = 1;

-- RESULT_QUERY
SELECT
  l.alert_event_id,
  u.id AS user_id,
  u.member_code,
  ar.rule_name,
  l.detected_at,
  l.status_event_type,
  l.from_status,
  l.to_status,
  l.reason,
  l.status_changed_at,
  l.delay_minutes,
  l.linked_event_rank,
  l.response_bucket
FROM tmp_alert_status_linked l
INNER JOIN users u ON u.id = l.user_id
INNER JOIN alert_rules ar ON ar.id = l.rule_id
ORDER BY l.delay_minutes ASC, l.detected_at DESC, l.alert_event_id;
