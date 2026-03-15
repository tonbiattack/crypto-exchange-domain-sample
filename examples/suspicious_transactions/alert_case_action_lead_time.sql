USE exchange_domain;

/*
  目的:
  - 検知からケース化、措置、ステータス変更までの各リードタイムを 1 行で確認する。
  - 運用SLAのどこで滞留しているかを切り分けるための SQL。

  見方:
  - alert_event_logs を起点に、紐づく suspicious_cases、account_actions、user_status_change_events を LEFT JOIN する。
  - case_open_delay_minutes は検知からケース起票まで。
  - action_delay_minutes は検知から最初の口座措置まで。
  - status_change_delay_minutes は検知から最初の状態変更まで。

  注意点:
  - 集計ダッシュボードでは期間条件をアプリ側から差し込む前提で使うのがよい。
*/
WITH first_actions AS (
  SELECT
    ael.id AS alert_event_id,
    MIN(aa.requested_at) AS first_action_at
  FROM alert_event_logs ael
  LEFT JOIN suspicious_cases sc ON sc.alert_event_log_id = ael.id
  LEFT JOIN account_actions aa ON aa.suspicious_case_id = sc.id
  GROUP BY ael.id
),
first_status_changes AS (
  SELECT
    ael.id AS alert_event_id,
    MIN(usce.occurred_at) AS first_status_changed_at
  FROM alert_event_logs ael
  LEFT JOIN user_status_change_events usce
    ON usce.user_id = ael.user_id
    AND usce.occurred_at >= ael.detected_at
    AND usce.occurred_at <= DATE_ADD(ael.detected_at, INTERVAL 7 DAY)
  GROUP BY ael.id
)
SELECT
  ael.id AS alert_event_id,
  u.id AS user_id,
  u.member_code,
  ar.rule_name,
  ael.detected_at,
  sc.id AS suspicious_case_id,
  sc.opened_at AS case_opened_at,
  fa.first_action_at,
  fsc.first_status_changed_at,
  TIMESTAMPDIFF(MINUTE, ael.detected_at, sc.opened_at) AS case_open_delay_minutes,
  TIMESTAMPDIFF(MINUTE, ael.detected_at, fa.first_action_at) AS action_delay_minutes,
  TIMESTAMPDIFF(MINUTE, ael.detected_at, fsc.first_status_changed_at) AS status_change_delay_minutes
FROM alert_event_logs ael
INNER JOIN users u ON u.id = ael.user_id
INNER JOIN alert_rules ar ON ar.id = ael.rule_id
LEFT JOIN suspicious_cases sc ON sc.alert_event_log_id = ael.id
LEFT JOIN first_actions fa ON fa.alert_event_id = ael.id
LEFT JOIN first_status_changes fsc ON fsc.alert_event_id = ael.id
ORDER BY ael.detected_at DESC, ael.id;
