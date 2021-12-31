{{ config(materialized='table') }}

WITH
  cases AS (
  SELECT
    id,
    source_of_infection,
    COALESCE(age_group, 'N/A') AS age_group
    COALESCE(neighbourhood_name, 'N/A') AS neighbourhood_name,
    classification,
    client_gender AS gender,
    episode_date,
    CASE
      WHEN currently_in_icu = "Yes" THEN 1
        ELSE 0
    END AS currently_in_icu
  FROM
    cases.staging_cases )

SELECT
  *
FROM
  cases