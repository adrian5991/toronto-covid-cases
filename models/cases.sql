WITH
  cases AS (
  SELECT DISTINCT
    id,
    source_of_infection,
    COALESCE(age_group, 'N/A') AS age_group,
    COALESCE(neighbourhood_name, 'N/A') AS neighbourhood_name,
    classification,
    client_gender AS gender,
    episode_date,
    CASE
      WHEN currently_in_icu = "Yes" THEN 1
        ELSE 0
    END AS currently_in_icu
  FROM
    cases.staging_cases 
  WHERE EXTRACT(MONTH FROM episode_date) = EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
)

  SELECT * FROM cases;