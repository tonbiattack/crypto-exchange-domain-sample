USE exchange_domain;

/*
  目的:
  - ユーザーの状態遷移をイベント理由つきで時系列表示し、監査や顧客問い合わせに対応しやすくする。
  - event と history に分かれた情報を1本で追えるようにする。
*/
SELECT
  -- 対象ユーザー。
  u.id AS user_id,
  u.member_code,
  -- 変更前後の状態。
  us_from.value AS from_status,
  us_to.value AS to_status,
  -- 遷移を起こしたイベント種別。
  et.value AS event_type,
  -- 実行主体。
  at.value AS actor_type,
  ev.actor_id,
  -- 変更理由。
  ev.reason,
  -- イベント発生時刻と状態反映時刻。
  ev.occurred_at,
  h.changed_at
FROM user_status_histories h
INNER JOIN user_status_change_events ev ON ev.id = h.status_change_event_id
INNER JOIN users u ON u.id = ev.user_id
LEFT JOIN user_statuses us_from ON us_from.id = h.from_status_id
INNER JOIN user_statuses us_to ON us_to.id = h.to_status_id
INNER JOIN user_status_event_types et ON et.id = ev.event_type_id
INNER JOIN actor_types at ON at.id = ev.actor_type_id
ORDER BY
  h.changed_at DESC,
  user_id;
