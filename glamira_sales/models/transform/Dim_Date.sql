WITH date_range AS (
    SELECT date AS date
    FROM UNNEST(GENERATE_DATE_ARRAY('2020-04-01', '2020-06-04')) AS date
),
dim_date AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(DAY FROM date) AS day,
        {{ dbt_utils.generate_surrogate_key(['date']) }} AS date_id,
        EXTRACT(DAYOFWEEK FROM date) AS day_of_week
    FROM date_range
)
SELECT * FROM dim_date