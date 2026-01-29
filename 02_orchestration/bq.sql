CREATE OR REPLACE EXTERNAL TABLE `project_id.dataset.2020_y`
OPTIONS (
  format = 'csv',
  uris = ['gs://bucket/yellow/2020*']
);

CREATE OR REPLACE EXTERNAL TABLE `project_id.dataset.2021_y`
OPTIONS (
  format = 'csv',
  uris = ['gs://bucket/yellow/2021-03*']
);

CREATE OR REPLACE EXTERNAL TABLE `project_id.dataset.2020_g`
OPTIONS (
  format = 'csv',
  uris = ['gs://bucket/green/2020*']
);

CREATE OR REPLACE EXTERNAL TABLE `project_id.dataset.2021_g`
OPTIONS (
  format = 'csv',
  uris = ['gs://bucket/green/2021*']
);

SELECT "2020_green"  AS dataset, COUNT(*) AS record_count FROM `project_id.dataset.2020_g`
UNION ALL
SELECT "2021_green",  COUNT(*) FROM `project_id.dataset.2021_g`
UNION ALL
SELECT "2020_yellow", COUNT(*) FROM `project_id.dataset.2020_y`
UNION ALL
SELECT "2021_yellow", COUNT(*) FROM `project_id.dataset.2021_y`;

-- Expected results:
-- | dataset        | record_count |
-- | -------------- | ------------ |
-- | 2021_green     | 570,466      |
-- | 2020_green     | 1,734,051    |
-- | 2021_yellow    | 1,925,152    |
-- | 2020_yellow    | 24,648,499   |