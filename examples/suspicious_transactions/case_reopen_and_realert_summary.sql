USE exchange_domain;

/*
  目的:
  - 一度クローズしたケースが再オープンされたり、再度アラート検知されたユーザーを集計する。
  - 早期クローズや継続監視の見直し対象を見つけるための SQL。

  見方:
  - case_status_histories から CLOSED -> それ以外 の遷移を reopen_count として数える。
  - alert_event_logs はケースの opened_at 以降の追加検知件数を re_alert_count として数える。
*/
WITH case_reopen_counts AS (
  SELECT
    sc.id AS suspicious_case_id,
    COUNT(*) AS reopen_count
  FROM suspicious_cases sc
  INNER JOIN case_status_histories csh ON csh.case_id = sc.id
  INNER JOIN case_statuses from_cs ON from_cs.id = csh.from_status_id
  INNER JOIN case_statuses to_cs ON to_cs.id = csh.to_status_id
  WHERE from_cs.value = 'CLOSED'
    AND to_cs.value <> 'CLOSED'
  GROUP BY sc.id
),
case_realert_counts AS (
  SELECT
    sc.id AS suspicious_case_id,
    COUNT(ael.id) AS re_alert_count
  FROM suspicious_cases sc
  LEFT JOIN alert_event_logs ael
    ON ael.user_id = sc.user_id
    AND ael.detected_at >= sc.opened_at
    AND (sc.closed_at IS NULL OR ael.detected_at >= sc.closed_at)
    AND (sc.alert_event_log_id IS NULL OR ael.id <> sc.alert_event_log_id)
  GROUP BY sc.id
)
SELECT
  sc.id AS suspicious_case_id,
  u.id AS user_id,
  u.member_code,
  sc.title,
  cs.value AS current_case_status,
  sc.opened_at,
  sc.closed_at,
  COALESCE(crc.reopen_count, 0) AS reopen_count,
  COALESCE(cac.re_alert_count, 0) AS re_alert_count
FROM suspicious_cases sc
INNER JOIN users u ON u.id = sc.user_id
INNER JOIN case_statuses cs ON cs.id = sc.current_status_id
LEFT JOIN case_reopen_counts crc ON crc.suspicious_case_id = sc.id
LEFT JOIN case_realert_counts cac ON cac.suspicious_case_id = sc.id
WHERE COALESCE(crc.reopen_count, 0) > 0
   OR COALESCE(cac.re_alert_count, 0) > 0
ORDER BY reopen_count DESC, re_alert_count DESC, sc.opened_at DESC;
