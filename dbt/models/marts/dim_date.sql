-- Calendar dimension covering the fetch window with headroom on both ends.
with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('2004-01-01', 'YYYY-MM-DD')",
        end_date="dateadd(year, 2, current_date())"
    ) }}
)
 
select
    date_day as date_key,
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    quarter(date_day) as quarter,
    dayofweek(date_day) as day_of_week
from spine