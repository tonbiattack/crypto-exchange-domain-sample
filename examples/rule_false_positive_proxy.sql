USE exchange_domain;

/*
  目的:
  - 誤検知が多そうなルールを proxy 指標で見つける。
  - 「ケース化されない」「IGNORED が多い」ルールを優先的に見直す。
*/
SELECT
  ar.id AS rule_id,
  ar.rule_name,
  ar.rule_type,
  ar.severity,
  COUNT(ael.id) AS detection_count,
  SUM(CASE WHEN aes.value = 'IGNORED' THEN 1 ELSE 0 END) AS ignored_count,
  SUM(CASE WHEN sc.id IS NULL THEN 1 ELSE 0 END) AS unlinked_count,
  ROUND(SUM(CASE WHEN aes.value = 'IGNORED' THEN 1 ELSE 0 END) / NULLIF(COUNT(ael.id), 0) * 100, 2) AS ignored_rate_pct,
  ROUND(SUM(CASE WHEN sc.id IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(ael.id), 0) * 100, 2) AS unlinked_rate_pct
FROM alert_rules ar
INNER JOIN alert_event_logs ael ON ael.rule_id = ar.id
INNER JOIN alert_event_statuses aes ON aes.id = ael.alert_event_status_id
LEFT JOIN suspicious_cases sc ON sc.alert_event_log_id = ael.id
GROUP BY
  ar.id,
  ar.rule_name,
  ar.rule_type,
  ar.severity
HAVING COUNT(ael.id) > 0
ORDER BY
  ignored_rate_pct DESC,
  unlinked_rate_pct DESC,
  detection_count DESC;
