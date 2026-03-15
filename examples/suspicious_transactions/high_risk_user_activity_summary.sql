USE exchange_domain;

/*
  目的:
  - HIGH / CRITICAL のケースを持つユーザーについて、活動量を横断的に確認する。
  - 再レビュー対象ユーザーの取引・入出金・検知・措置を1行で見たい時に使う。

  見方:
  - high_risk_users で「高リスクケースを持つユーザー集合」を先に作る。
  - その後、約定件数、入出金件数、検知件数、措置件数を LEFT JOIN で横持ちにする。
  - max_risk_level は、そのユーザーが持つケースのうち最も高いリスクを表す。
*/
WITH high_risk_users AS (
  SELECT
    sc.user_id,
    -- CRITICAL を 2、HIGH を 1 として最大値を取り、ユーザーの最大リスクを求める。
    MAX(CASE rl.value WHEN 'CRITICAL' THEN 2 WHEN 'HIGH' THEN 1 ELSE 0 END) AS risk_rank,
    -- 同じユーザーに紐づくケース件数。
    COUNT(DISTINCT sc.id) AS case_count,
    -- OPEN / INVESTIGATING のケースを未クローズとして数える。
    SUM(CASE WHEN cs.value IN ('OPEN', 'INVESTIGATING') THEN 1 ELSE 0 END) AS open_case_count,
    -- 最後に起票されたケース日時。
    MAX(sc.opened_at) AS last_case_opened_at
  FROM suspicious_cases sc
  INNER JOIN risk_levels rl ON rl.id = sc.risk_level_id
  INNER JOIN case_statuses cs ON cs.id = sc.current_status_id
  WHERE rl.value IN ('HIGH', 'CRITICAL')
  GROUP BY sc.user_id
),
trade_counts AS (
  -- ユーザー別の約定件数。
  SELECT user_id, COUNT(*) AS execution_count
  FROM trade_executions
  GROUP BY user_id
),
fiat_deposit_counts AS (
  -- 完了済み法定入金件数。
  SELECT fd.user_id, COUNT(*) AS fiat_deposit_count
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  WHERE ds.value = 'COMPLETED' AND fd.completed_at IS NOT NULL
  GROUP BY fd.user_id
),
fiat_withdrawal_counts AS (
  -- 完了済み法定出金件数。
  SELECT fw.user_id, COUNT(*) AS fiat_withdrawal_count
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED' AND fw.completed_at IS NOT NULL
  GROUP BY fw.user_id
),
alert_counts AS (
  -- 検知件数と直近検知時刻。
  SELECT user_id, COUNT(*) AS alert_count, MAX(detected_at) AS last_alert_at
  FROM alert_event_logs
  GROUP BY user_id
),
action_counts AS (
  -- 口座措置件数。
  SELECT user_id, COUNT(*) AS action_count
  FROM account_actions
  GROUP BY user_id
)
SELECT
  -- 顧客レビューの主キー情報。
  u.id AS user_id,
  u.member_code,
  us.value AS current_status,
  -- CASE式で risk_rank を表示用の文字列へ戻している。
  CASE hru.risk_rank WHEN 2 THEN 'CRITICAL' ELSE 'HIGH' END AS max_risk_level,
  hru.case_count,
  hru.open_case_count,
  -- LEFT JOIN 先に行が無いユーザーでも 0 件として表示する。
  COALESCE(tc.execution_count, 0) AS execution_count,
  COALESCE(fdc.fiat_deposit_count, 0) AS fiat_deposit_count,
  COALESCE(fwc.fiat_withdrawal_count, 0) AS fiat_withdrawal_count,
  COALESCE(ac.alert_count, 0) AS alert_count,
  COALESCE(aac.action_count, 0) AS action_count,
  hru.last_case_opened_at,
  ac.last_alert_at
FROM high_risk_users hru
INNER JOIN users u ON u.id = hru.user_id
INNER JOIN user_statuses us ON us.id = u.current_status_id
LEFT JOIN trade_counts tc ON tc.user_id = hru.user_id
LEFT JOIN fiat_deposit_counts fdc ON fdc.user_id = hru.user_id
LEFT JOIN fiat_withdrawal_counts fwc ON fwc.user_id = hru.user_id
LEFT JOIN alert_counts ac ON ac.user_id = hru.user_id
LEFT JOIN action_counts aac ON aac.user_id = hru.user_id
-- リスクが高く、かつ未クローズケースが多いユーザーを先頭に出す。
ORDER BY hru.risk_rank DESC, hru.open_case_count DESC, ac.last_alert_at DESC, u.id;
