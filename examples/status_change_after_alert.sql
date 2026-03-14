USE exchange_domain;

/*
  目的:
  - アラート検知後に実施されたユーザーステータス変更を追跡する。
  - 検知から凍結などの措置まで、どれだけ早く運用が動いたかを確認する。

  見方:
  - alert_event_logs.detected_at 以降 72時間以内の user_status_change_events を結び付ける。
  - delay_minutes は検知から状態変更イベントまでの経過分。
  - user_status_histories があれば、ACTIVE -> FROZEN のような遷移前後も見られる。
*/
SELECT
  -- どのアラートを起点にした措置か。
  ael.id AS alert_event_id,
  u.id AS user_id,
  u.member_code,
  ar.rule_name,
  ael.detected_at,
  -- 変更イベント種別。例: FROZEN, WITHDRAWN。
  uset.value AS status_event_type,
  -- ステータス履歴がある場合は変更前後の状態を返す。
  from_us.value AS from_status,
  to_us.value AS to_status,
  -- なぜ状態変更したか。
  usce.reason,
  usce.occurred_at AS status_changed_at,
  -- 検知から状態変更までの分数。
  TIMESTAMPDIFF(MINUTE, ael.detected_at, usce.occurred_at) AS delay_minutes
FROM alert_event_logs ael
INNER JOIN users u ON u.id = ael.user_id
INNER JOIN alert_rules ar ON ar.id = ael.rule_id
-- 検知後 72時間以内に発生した状態変更だけを関連措置として扱う。
INNER JOIN user_status_change_events usce
  ON usce.user_id = ael.user_id
 AND usce.occurred_at >= ael.detected_at
 AND usce.occurred_at <= DATE_ADD(ael.detected_at, INTERVAL 72 HOUR)
INNER JOIN user_status_event_types uset ON uset.id = usce.event_type_id
-- 履歴が無いイベントも落とさないよう LEFT JOIN にしている。
LEFT JOIN user_status_histories ush ON ush.status_change_event_id = usce.id AND ush.user_id = ael.user_id
LEFT JOIN user_statuses from_us ON from_us.id = ush.from_status_id
LEFT JOIN user_statuses to_us ON to_us.id = ush.to_status_id
-- 初動が早い措置を先に確認できるよう delay_minutes 昇順。
ORDER BY delay_minutes ASC, ael.detected_at DESC, ael.id;
