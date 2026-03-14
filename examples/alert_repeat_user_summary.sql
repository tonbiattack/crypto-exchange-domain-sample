USE exchange_domain;

/*
  目的:
  - 同一ユーザーに対する繰り返し検知を集計し、継続監視が必要な対象を見つける。
  - 単一ルールではなく、複数回・複数ルールで検知されるユーザーを優先確認する。

  見方:
  - detection_count は総検知件数。
  - distinct_rule_count は何種類のルールで引っかかったか。
  - linked_case_count はケース化まで進んだ件数。
*/
SELECT
  -- どのユーザーが繰り返し検知されているか。
  u.id AS user_id,
  u.member_code,
  -- 総検知件数。
  COUNT(ael.id) AS detection_count,
  -- 同一ルールの連発ではなく、複数ルール横断での検知広がりを見る。
  COUNT(DISTINCT ael.rule_id) AS distinct_rule_count,
  -- 同一アラートに複数ケースがぶら下がっても重複しないよう DISTINCT を使う。
  COUNT(DISTINCT sc.id) AS linked_case_count,
  -- いつからいつまで検知が続いているか。
  MIN(ael.detected_at) AS first_detected_at,
  MAX(ael.detected_at) AS last_detected_at
FROM alert_event_logs ael
INNER JOIN users u ON u.id = ael.user_id
LEFT JOIN suspicious_cases sc ON sc.alert_event_log_id = ael.id
GROUP BY u.id, u.member_code
-- 単発検知は除外し、2回以上の再発ユーザーだけを対象にする。
HAVING COUNT(ael.id) >= 2
ORDER BY detection_count DESC, distinct_rule_count DESC, last_detected_at DESC, u.id;
