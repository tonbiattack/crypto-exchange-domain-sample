USE exchange_domain;
SET NAMES utf8mb4;

INSERT INTO occupations (name, value, description) VALUES
  ('会社員', 'EMPLOYEE', '企業等に雇用されている就業者'),
  ('公務員', 'PUBLIC_SERVANT', '官公庁・自治体等に勤務する就業者'),
  ('経営者', 'EXECUTIVE', '法人の経営に従事する者'),
  ('個人事業主', 'SELF_EMPLOYED', '自営で事業を行う者'),
  ('自由業', 'FREELANCER', '専門職等で業務委託中心に働く者'),
  ('パート・アルバイト', 'PART_TIME', '短時間勤務・非正規雇用の就業者'),
  ('専業主婦・専業主夫', 'HOMEMAKER', '家事に専従している者'),
  ('学生', 'STUDENT', '学生・院生等'),
  ('年金受給者', 'PENSIONER', '主な収入が年金の者'),
  ('無職', 'UNEMPLOYED', '就業していない者'),
  ('医師・医療従事者', 'MEDICAL', '医療分野の就業者'),
  ('教職員', 'EDUCATION', '教育機関の就業者'),
  ('ITエンジニア', 'IT_ENGINEER', 'IT関連の技術職'),
  ('金融業', 'FINANCE', '金融機関・金融関連企業の就業者'),
  ('その他', 'OTHER', '上記に該当しない職業')
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  description = VALUES(description),
  updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO annual_income_brackets (name, value, description) VALUES
  ('300万円未満', 'LT_3M', '年間所得が300万円未満'),
  ('300万円以上500万円未満', '3M_TO_5M', '年間所得が300万円以上500万円未満'),
  ('500万円以上700万円未満', '5M_TO_7M', '年間所得が500万円以上700万円未満'),
  ('700万円以上1000万円未満', '7M_TO_10M', '年間所得が700万円以上1000万円未満'),
  ('1000万円以上1500万円未満', '10M_TO_15M', '年間所得が1000万円以上1500万円未満'),
  ('1500万円以上3000万円未満', '15M_TO_30M', '年間所得が1500万円以上3000万円未満'),
  ('3000万円以上', 'GTE_30M', '年間所得が3000万円以上')
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  description = VALUES(description),
  updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO financial_asset_brackets (name, value, description) VALUES
  ('100万円未満', 'LT_1M', '金融資産が100万円未満'),
  ('100万円以上300万円未満', '1M_TO_3M', '金融資産が100万円以上300万円未満'),
  ('300万円以上500万円未満', '3M_TO_5M', '金融資産が300万円以上500万円未満'),
  ('500万円以上1000万円未満', '5M_TO_10M', '金融資産が500万円以上1000万円未満'),
  ('1000万円以上3000万円未満', '10M_TO_30M', '金融資産が1000万円以上3000万円未満'),
  ('3000万円以上5000万円未満', '30M_TO_50M', '金融資産が3000万円以上5000万円未満'),
  ('5000万円以上1億円未満', '50M_TO_100M', '金融資産が5000万円以上1億円未満'),
  ('1億円以上', 'GTE_100M', '金融資産が1億円以上')
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  description = VALUES(description),
  updated_at = CURRENT_TIMESTAMP(6);
