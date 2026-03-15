USE exchange_domain;

/*
  目的:
  - 同一ユーザーが同一送金先アドレスへ短時間に出金を分割している候補を抽出する。
  - いわゆる structuring / split withdrawal の一次スクリーニングに使う。

  想定シナリオ:
  - 直近24時間に completed な暗号資産出金が3件以上ある。
  - 同一送金先への出金が6時間以内にまとまっている。

  注意点:
  - 同一通貨・同一アドレスでのみ集計する。
  - 閾値(24時間、3件、6時間)は運用ルールに合わせて調整する。
*/
SELECT
  u.id AS user_id,
  u.member_code,
  c.code AS currency_code,
  cw.destination_address,
  COUNT(*) AS split_withdrawal_count,
  SUM(cw.amount) AS total_amount,
  MIN(cw.completed_at) AS first_withdrawal_at,
  MAX(cw.completed_at) AS last_withdrawal_at,
  TIMESTAMPDIFF(MINUTE, MIN(cw.completed_at), MAX(cw.completed_at)) AS spread_minutes
FROM crypto_withdrawals cw
INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
INNER JOIN users u ON u.id = cw.user_id
INNER JOIN currencies c ON c.id = cw.currency_id
WHERE
  ws.value = 'COMPLETED'
  AND cw.completed_at IS NOT NULL
  AND cw.completed_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 24 HOUR)
GROUP BY
  u.id,
  u.member_code,
  c.code,
  cw.destination_address
HAVING
  COUNT(*) >= 3
  AND TIMESTAMPDIFF(HOUR, MIN(cw.completed_at), MAX(cw.completed_at)) <= 6
ORDER BY
  split_withdrawal_count DESC,
  total_amount DESC,
  last_withdrawal_at DESC,
  user_id;
