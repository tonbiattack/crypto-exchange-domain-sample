USE exchange_domain;

/*
  目的:
  - 検知からケース起票までの初動時間を把握し、運用SLAの確認に使う。

  見方:
  - alert_event_logs.detected_at を検知時刻、
    suspicious_cases.opened_at をケース起票時刻として比較する。
  - たとえば detected_at から 30分後に opened_at が入っていれば、
    avg_conversion_minutes / max_conversion_minutes / min_conversion_minutes は 30 になる。
*/
SELECT
  -- どのルールで検知されたケース群か。
  ar.rule_name,
  -- CRITICAL / HIGH などの重要度。
  ar.severity,
  -- 検知のうち、実際にケースへ変換された件数。
  COUNT(sc.id) AS converted_case_count,
  -- 検知からケース起票までの平均所要分。
  -- TIMESTAMPDIFF(MINUTE, ael.detected_at, sc.opened_at) を平均している。
  ROUND(AVG(TIMESTAMPDIFF(MINUTE, ael.detected_at, sc.opened_at)), 2) AS avg_conversion_minutes,
  -- 最も時間がかかったケース化までの所要分。
  MAX(TIMESTAMPDIFF(MINUTE, ael.detected_at, sc.opened_at)) AS max_conversion_minutes,
  -- 最も早くケース化された所要分。
  MIN(TIMESTAMPDIFF(MINUTE, ael.detected_at, sc.opened_at)) AS min_conversion_minutes
FROM alert_event_logs ael
INNER JOIN alert_rules ar ON ar.id = ael.rule_id
-- ケース化されていないアラートは除外し、「起票済みだけ」を集計する。
INNER JOIN suspicious_cases sc ON sc.alert_event_log_id = ael.id
-- ルールごとの初動時間を見るため、rule_name と severity 単位で集約する。
GROUP BY ar.rule_name, ar.severity
ORDER BY avg_conversion_minutes DESC, converted_case_count DESC;
