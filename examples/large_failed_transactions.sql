USE exchange_domain;

/*
  目的:
  - 高額な失敗取引を業務横断で抽出し、優先調査対象を見つける。
  - 顧客影響が大きい失敗を先に確認できるようにする。
*/
SELECT
  operation_type,
  user_id,
  member_code,
  currency_code,
  amount,
  requested_at,
  failed_at,
  public_id
FROM (
  SELECT
    'FIAT_DEPOSIT' AS operation_type,
    fd.user_id,
    u.member_code,
    c.code AS currency_code,
    fd.amount,
    fd.requested_at,
    fd.failed_at,
    fd.public_deposit_hash AS public_id
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  INNER JOIN users u ON u.id = fd.user_id
  INNER JOIN currencies c ON c.id = fd.currency_id
  WHERE ds.value = 'FAILED'

  UNION ALL

  SELECT
    'FIAT_WITHDRAWAL' AS operation_type,
    fw.user_id,
    u.member_code,
    c.code AS currency_code,
    fw.amount,
    fw.requested_at,
    fw.failed_at,
    fw.public_withdrawal_hash AS public_id
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  INNER JOIN users u ON u.id = fw.user_id
  INNER JOIN currencies c ON c.id = fw.currency_id
  WHERE ws.value = 'FAILED'

  UNION ALL

  SELECT
    'CRYPTO_DEPOSIT' AS operation_type,
    cd.user_id,
    u.member_code,
    c.code AS currency_code,
    cd.amount,
    cd.detected_at AS requested_at,
    cd.failed_at,
    cd.public_deposit_hash AS public_id
  FROM crypto_deposits cd
  INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
  INNER JOIN users u ON u.id = cd.user_id
  INNER JOIN currencies c ON c.id = cd.currency_id
  WHERE ds.value = 'FAILED'

  UNION ALL

  SELECT
    'CRYPTO_WITHDRAWAL' AS operation_type,
    cw.user_id,
    u.member_code,
    c.code AS currency_code,
    cw.amount,
    cw.requested_at,
    cw.failed_at,
    cw.public_withdrawal_hash AS public_id
  FROM crypto_withdrawals cw
  INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
  INNER JOIN users u ON u.id = cw.user_id
  INNER JOIN currencies c ON c.id = cw.currency_id
  WHERE ws.value = 'FAILED'
) t
ORDER BY
  amount DESC,
  failed_at DESC;
