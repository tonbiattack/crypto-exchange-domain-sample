USE exchange_domain;

/*
  目的:
  - 口座措置の実行件数を日別・措置種別別に集計し、運用負荷を把握する。
  - 管理者/システムどちらの措置が多いかも確認する。
*/
SELECT
  DATE(aa.requested_at) AS action_date,
  aat.value AS action_type,
  at.value AS actor_type,
  COUNT(*) AS action_count,
  SUM(CASE WHEN aa.completed_at IS NOT NULL THEN 1 ELSE 0 END) AS completed_count,
  MIN(aa.requested_at) AS first_requested_at,
  MAX(aa.requested_at) AS last_requested_at
FROM account_actions aa
INNER JOIN account_action_types aat ON aat.id = aa.action_type_id
INNER JOIN actor_types at ON at.id = aa.actor_type_id
GROUP BY
  DATE(aa.requested_at),
  aat.value,
  at.value
ORDER BY
  action_date DESC,
  action_count DESC,
  action_type;
