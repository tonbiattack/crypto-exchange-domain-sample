USE exchange_domain;

/*
  目的:
  - 入出金ごとの申請から完了までの所要時間を、業務種別別・日別に把握する。
  - オペレーション遅延やチェーン確認遅延の兆候を日次で確認する。
*/
SELECT
  -- 業務種別。法定/暗号資産、入金/出金を同じ軸で比較する。
  operation_type,
  -- 「申請日」または「検知日」を日次軸として採用。
  activity_date,
  -- 完了済みデータの件数。
  COUNT(*) AS completed_count,
  -- 平均所要時間(分)。
  ROUND(AVG(lead_minutes), 2) AS avg_lead_minutes,
  -- 最長所要時間(分)。
  MAX(lead_minutes) AS max_lead_minutes,
  -- 最短所要時間(分)。
  MIN(lead_minutes) AS min_lead_minutes
FROM (
  SELECT
    -- 法定入金は requested_at -> completed_at の差を使う。
    'FIAT_DEPOSIT' AS operation_type,
    DATE(fd.requested_at) AS activity_date,
    TIMESTAMPDIFF(MINUTE, fd.requested_at, fd.completed_at) AS lead_minutes
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  WHERE ds.value = 'COMPLETED' AND fd.completed_at IS NOT NULL

  UNION ALL

  SELECT
    -- 法定出金は requested_at -> completed_at の差を使う。
    'FIAT_WITHDRAWAL' AS operation_type,
    DATE(fw.requested_at) AS activity_date,
    TIMESTAMPDIFF(MINUTE, fw.requested_at, fw.completed_at) AS lead_minutes
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED' AND fw.completed_at IS NOT NULL

  UNION ALL

  SELECT
    -- 暗号資産入金は detected_at -> confirmed_at をリードタイムとみなす。
    'CRYPTO_DEPOSIT' AS operation_type,
    DATE(cd.detected_at) AS activity_date,
    TIMESTAMPDIFF(MINUTE, cd.detected_at, cd.confirmed_at) AS lead_minutes
  FROM crypto_deposits cd
  INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
  WHERE ds.value = 'COMPLETED' AND cd.confirmed_at IS NOT NULL

  UNION ALL

  SELECT
    -- 暗号資産出金は requested_at -> completed_at の差を使う。
    'CRYPTO_WITHDRAWAL' AS operation_type,
    DATE(cw.requested_at) AS activity_date,
    TIMESTAMPDIFF(MINUTE, cw.requested_at, cw.completed_at) AS lead_minutes
  FROM crypto_withdrawals cw
  INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED' AND cw.completed_at IS NOT NULL
) t
-- UNION ALL で正規化した完了データを 業務種別 x 日付 で再集計する。
GROUP BY operation_type, activity_date
ORDER BY activity_date DESC, operation_type;
