USE exchange_domain;

/*
  目的:
  - 同一送金先アドレスに対して、短期間に複数ユーザーから出金が集まっているクラスタを集計する。
  - destination_address の使い回しを、期間集中の観点で強化した版。

  見方:
  - completed な crypto_withdrawals を対象に、destination_address 単位で集約する。
  - 直近30日を例に cluster_start_at / cluster_end_at を出し、期間内の集中度を確認する。

  注意点:
  - 30日は例示条件。運用ではアプリ側パラメータで変えられるようにするのがよい。
*/
SELECT
  cw.destination_address,
  COUNT(*) AS withdrawal_count,
  COUNT(DISTINCT cw.user_id) AS user_count,
  SUM(cw.amount) AS total_amount,
  MIN(cw.completed_at) AS cluster_start_at,
  MAX(cw.completed_at) AS cluster_end_at,
  GROUP_CONCAT(DISTINCT u.member_code ORDER BY u.member_code SEPARATOR ',') AS member_codes
FROM crypto_withdrawals cw
INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
INNER JOIN users u ON u.id = cw.user_id
WHERE ws.value = 'COMPLETED'
  AND cw.completed_at IS NOT NULL
  AND cw.completed_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY)
GROUP BY cw.destination_address
HAVING COUNT(DISTINCT cw.user_id) >= 2
ORDER BY user_count DESC, withdrawal_count DESC, total_amount DESC, cluster_end_at DESC;
