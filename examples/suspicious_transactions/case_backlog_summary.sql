USE exchange_domain;

/*
  目的:
  - ケースの滞留件数をステータス別・リスク別に把握し、調査 backlog を俯瞰する。
  - どのリスク帯で滞留が発生しているかを一目で確認する。

  設計意図:
  - suspicious_cases を現在値として扱い、case_statuses / risk_levels と結合して集計する。
  - closed_at が NULL のケースだけを backlog とみなし、未完了のケースに絞る。
  - DATEDIFF で経過日数を算出し、平均滞留日数と最長滞留日数を返す。

  出力の読み方:
  - backlog_count が多い組み合わせは、調査資源が不足している候補。
  - oldest_open_days が大きい場合、SLA 違反や長期滞留の可能性がある。
*/
SELECT
  -- 現在のケースステータス。
  cs.value AS case_status,
  -- 現在のリスクレベル。
  rl.value AS risk_level,
  -- 未完了ケース数。
  COUNT(*) AS backlog_count,
  -- 平均滞留日数。
  ROUND(AVG(DATEDIFF(CURRENT_DATE, DATE(sc.opened_at))), 2) AS avg_open_days,
  -- 最長滞留日数。
  MAX(DATEDIFF(CURRENT_DATE, DATE(sc.opened_at))) AS oldest_open_days,
  -- 最も古い起票日時。
  MIN(sc.opened_at) AS oldest_opened_at,
  -- 最も新しい起票日時。
  MAX(sc.opened_at) AS newest_opened_at
FROM suspicious_cases sc
INNER JOIN case_statuses cs ON cs.id = sc.current_status_id
INNER JOIN risk_levels rl ON rl.id = sc.risk_level_id
WHERE
  -- 未クローズのケースだけを backlog とみなす。
  sc.closed_at IS NULL
GROUP BY
  cs.value,
  rl.value
ORDER BY
  -- 高リスクを先に見る。
  CASE rl.value
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    WHEN 'MEDIUM' THEN 3
    WHEN 'LOW' THEN 4
    ELSE 5
  END,
  backlog_count DESC,
  cs.value;
