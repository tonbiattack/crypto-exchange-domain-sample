USE exchange_domain;

/*
  目的:
  - ケースの起票からクローズまでの所要時間を確認し、対応SLAを評価する。
  - クローズ済みケースだけを対象にし、平均/最長のリードタイムを返す。
*/
SELECT
  -- リスク別に集計。
  rl.value AS risk_level,
  -- クローズ済み件数。
  COUNT(*) AS closed_case_count,
  -- 平均リードタイム(日)。
  ROUND(AVG(TIMESTAMPDIFF(HOUR, sc.opened_at, sc.closed_at)) / 24, 2) AS avg_lead_days,
  -- 最長リードタイム(日)。
  ROUND(MAX(TIMESTAMPDIFF(HOUR, sc.opened_at, sc.closed_at)) / 24, 2) AS max_lead_days,
  -- 最短リードタイム(日)。
  ROUND(MIN(TIMESTAMPDIFF(HOUR, sc.opened_at, sc.closed_at)) / 24, 2) AS min_lead_days
FROM suspicious_cases sc
INNER JOIN risk_levels rl ON rl.id = sc.risk_level_id
WHERE sc.closed_at IS NOT NULL
GROUP BY rl.value
ORDER BY
  CASE rl.value
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    WHEN 'MEDIUM' THEN 3
    WHEN 'LOW' THEN 4
    ELSE 5
  END;
