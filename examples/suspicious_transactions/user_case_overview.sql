USE exchange_domain;

/*
  目的:
  - ユーザーごとの検知件数、ケース件数、口座措置件数、現在ステータスを1行で把握する。
  - AML 顧客レビュー画面の簡易一覧として使う。
*/
SELECT
  -- レビュー対象ユーザー。
  u.id AS user_id,
  u.member_code,
  -- users.current_status_id にぶら下がる現在ステータス。
  us.value AS current_status,
  -- ユーザーに紐づく検知イベント件数。
  COUNT(DISTINCT ael.id) AS alert_count,
  -- ユーザーに紐づくケース件数。
  COUNT(DISTINCT sc.id) AS case_count,
  -- ユーザーに紐づく口座措置件数。
  COUNT(DISTINCT aa.id) AS account_action_count,
  -- 最終検知日時。
  MAX(ael.detected_at) AS last_alert_at,
  -- 最終ケース起票日時。
  MAX(sc.opened_at) AS last_case_opened_at,
  -- 最終措置依頼日時。
  MAX(aa.requested_at) AS last_action_requested_at
FROM users u
INNER JOIN user_statuses us ON us.id = u.current_status_id
-- 0件ユーザーも残したいので LEFT JOIN。
LEFT JOIN alert_event_logs ael ON ael.user_id = u.id
LEFT JOIN suspicious_cases sc ON sc.user_id = u.id
LEFT JOIN account_actions aa ON aa.user_id = u.id
-- ael/sc/aa を同時JOINすると行が増幅するため COUNT(DISTINCT ...) を使う。
GROUP BY u.id, u.member_code, us.value
ORDER BY case_count DESC, alert_count DESC, user_id;
