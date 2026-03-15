USE exchange_domain;

/*
  目的:
  - ルールごとの検知件数、未処理件数、ケース化率を一覧化し、検知ルールの運用品質を確認する。
  - スコアや重要度とあわせて「よく当たるルール」「放置されやすいルール」を見つける。

  設計意図:
  - alert_event_logs を起点に alert_rules、alert_event_statuses、suspicious_cases を結合する。
  - suspicious_cases は LEFT JOIN にして、未ケース化の検知イベントも集計に含める。
  - case_link_rate_pct はケース化割合、open_alert_count は未クローズ件数の近似指標として使う。

  出力の読み方:
  - detection_count が多いのに case_link_rate_pct が低いルールは、誤検知過多か閾値過敏の可能性がある。
  - open_alert_count が多いルールは、運用負荷が高い候補。
*/
SELECT
  -- ルール識別子。
  ar.id AS rule_id,
  -- 運用画面向けルール名。
  ar.rule_name,
  -- ルールのカテゴリ。
  ar.rule_type,
  -- 重要度。
  ar.severity,
  -- そのルールで発生した検知総数。
  COUNT(ael.id) AS detection_count,
  -- 未対応/確認済み/無視を含む現在未クローズ相当の件数。
  SUM(CASE WHEN aes.value <> 'IGNORED' THEN 1 ELSE 0 END) AS active_alert_count,
  -- ケースに紐づいた検知件数。
  SUM(CASE WHEN sc.id IS NOT NULL THEN 1 ELSE 0 END) AS linked_case_count,
  -- ケース化率(%)
  ROUND(
    SUM(CASE WHEN sc.id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(ael.id), 0) * 100,
    2
  ) AS case_link_rate_pct,
  -- 検知スコアの平均。
  ROUND(AVG(ael.score), 4) AS avg_score,
  -- 初回検知日時。
  MIN(ael.detected_at) AS first_detected_at,
  -- 最終検知日時。
  MAX(ael.detected_at) AS last_detected_at
FROM alert_rules ar
INNER JOIN alert_event_logs ael ON ael.rule_id = ar.id
INNER JOIN alert_event_statuses aes ON aes.id = ael.alert_event_status_id
LEFT JOIN suspicious_cases sc ON sc.alert_event_log_id = ael.id
GROUP BY
  ar.id,
  ar.rule_name,
  ar.rule_type,
  ar.severity
ORDER BY
  -- まず重要度順。
  CASE ar.severity
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    WHEN 'MEDIUM' THEN 3
    WHEN 'LOW' THEN 4
    ELSE 5
  END,
  -- 同重要度内では件数が多いルールを先頭に。
  detection_count DESC,
  ar.rule_name;
