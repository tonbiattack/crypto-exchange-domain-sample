USE exchange_domain;

/*
  目的:
  - 自動検知イベントのうち、未対応または対応中のものをケース状況と合わせて一覧化する。
  - AML/不正対策オペレーションで、次に見るべきアラートを優先順位付きで確認する。

  設計意図:
  - alert_event_logs を中心に alert_rules と suspicious_cases を突き合わせる。
  - ケース未起票のイベントも残すため、suspicious_cases は LEFT JOIN にする。
  - 高スコア・高重要度・新しい検知を優先表示する。

  出力の読み方:
  - linked_case_id が NULL なら、まだケース化されていない検知イベント。
  - case_status が OPEN/INVESTIGATING 相当で長く滞留している場合は運用ボトルネック候補。
*/
SELECT
  -- 検知イベントの主キー。
  ael.id AS alert_event_id,
  -- 検知対象ユーザー。
  u.id AS user_id,
  u.member_code,
  -- どのルールに引っかかったか。
  ar.rule_name,
  ar.rule_type,
  ar.severity,
  -- 検知イベントのワークフロー上の状態。
  aes.value AS alert_event_status,
  -- スコアが高いほど優先的に確認したい想定。
  ael.score,
  ael.detected_at,
  -- 対象業務IDを列ごとに確認できるよう残す。
  ael.trade_execution_id,
  ael.fiat_deposit_id,
  ael.fiat_withdrawal_id,
  ael.crypto_deposit_id,
  ael.crypto_withdrawal_id,
  -- 紐づくケース情報。未起票なら NULL。
  sc.id AS linked_case_id,
  cs.value AS case_status,
  rl.value AS risk_level,
  sc.opened_at AS case_opened_at,
  sc.closed_at AS case_closed_at,
  ael.note
FROM alert_event_logs ael
INNER JOIN users u ON u.id = ael.user_id
INNER JOIN alert_rules ar ON ar.id = ael.rule_id
INNER JOIN alert_event_statuses aes ON aes.id = ael.alert_event_status_id
LEFT JOIN suspicious_cases sc ON sc.alert_event_log_id = ael.id
LEFT JOIN case_statuses cs ON cs.id = sc.current_status_id
LEFT JOIN risk_levels rl ON rl.id = sc.risk_level_id
WHERE
  -- クローズ済み検知イベントは除外し、対応待ちだけを見る想定。
  aes.value <> 'CLOSED'
ORDER BY
  -- 重要度の高いものを優先。
  CASE ar.severity
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    WHEN 'MEDIUM' THEN 3
    WHEN 'LOW' THEN 4
    ELSE 5
  END,
  -- 同重要度ならスコア高い順。
  ael.score DESC,
  -- さらに新しい検知を先頭へ。
  ael.detected_at DESC;
