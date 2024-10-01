SELECT
  date
  , FORMAT_DATETIME ("%A", date) AS day_of_week
  , FORMAT_DATETIME ("%a", date) AS day_of_week_short
  , CASE 
      WHEN FORMAT_DATETIME ("%a", date) IN ('Sat', 'Sun')
      THEN 'Weekend'
      ELSE 'Weekday'
    END AS is_weekday_or_weekend
  , DATE_TRUNC(date, MONTH) as year_month
  , FORMAT_DATETIME ("%B", date) AS month
  , DATE_TRUNC(date, YEAR) as year
  , EXTRACT(YEAR FROM date) AS year_number

FROM UNNEST(GENERATE_DATE_ARRAY('2013-01-01', '2030-12-31')) AS date
ORDER BY 1

