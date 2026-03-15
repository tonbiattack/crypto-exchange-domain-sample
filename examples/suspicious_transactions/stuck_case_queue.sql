USE exchange_domain;

/*
  目的:
  - 調査中や措置待ちのまま長く滞留しているケースを一覧で返す。
  - 集計ではなく、現場オペレーションで次に触る案件を探すためのキュー。

  見方:
  - INVESTIGATING / ACTION_REQUIRED を対象にする。
  - pending_case_minutes は opened_at から現在まで、もしくは直近ステータス変更から現在までの経過時間。
  - assigned_to が NULL なら未アサイン案件。

  注意点:
  - 48時間は例示閾値。実務ではアプリ側パラメータ化を推奨。
*/
WITH latest_case_activity AS (
  SELECT
    sc.id AS suspicious_case_id,
    COALESCE(MAX(csh.changed_at), sc.opened_at) AS last_activity_at
  FROM suspicious_cases sc
  LEFT JOIN case_status_histories csh ON csh.case_id = sc.id
  GROUP BY sc.id, sc.opened_at
)
SELECT
  sc.id AS suspicious_case_id,
  u.id AS user_id,
  u.member_code,
  sc.title,
  cs.value AS current_case_status,
  rl.value AS risk_level,
  sc.assigned_to,
  lca.last_activity_at,
  TIMESTAMPDIFF(MINUTE, lca.last_activity_at, CURRENT_TIMESTAMP) AS pending_case_minutes
FROM suspicious_cases sc
INNER JOIN users u ON u.id = sc.user_id
INNER JOIN case_statuses cs ON cs.id = sc.current_status_id
INNER JOIN risk_levels rl ON rl.id = sc.risk_level_id
INNER JOIN latest_case_activity lca ON lca.suspicious_case_id = sc.id
WHERE cs.value IN ('INVESTIGATING', 'ACTION_REQUIRED')
  AND TIMESTAMPDIFF(HOUR, lca.last_activity_at, CURRENT_TIMESTAMP) >= 48
ORDER BY pending_case_minutes DESC, rl.value DESC, sc.opened_at ASC;
