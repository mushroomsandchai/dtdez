select '2020_green', count(*) from `project_id.dataset.green_2020`
union all
select '2020_yellow', count(*) from `project_id.dataset.yellow_2020`
union all
select '2021_yellow', count(*) from `project_id.dataset.yellow_2021`;

-- Expected results:
-- | dataset        | record_count |
-- | -------------- | ------------ |
-- | 2020_green     | 1,734,051    |
-- | 2021_yellow    | 1,925,152    |
-- | 2020_yellow    | 24,648,499   |