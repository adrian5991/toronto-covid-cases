with icu as (
    select
        episode_date, 
        SUM(currently_in_icu) AS num_in_icu
    FROM cases.cases
    GROUP BY
        episode_date
)

select * from icu