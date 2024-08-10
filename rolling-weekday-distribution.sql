/* TODO confirm range, i think add other columns too. */
SELECT
  epoch_week,
  EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
  sales,
  SUM(sales) OVER daily_sales AS total_sales_per_day,
  SUM(sales) OVER rolling_sales AS total_sales_last_4_weeks,
  (SUM(sales) OVER daily_sales / SUM(sales) OVER rolling_sales) AS proportion_of_4_week_sales
FROM
  sales_table
WINDOW
  daily_sales AS (PARTITION BY epoch_week, EXTRACT(DAYOFWEEK FROM date)),
  rolling_sales AS (ORDER BY epoch_week RANGE BETWEEN 3 PRECEDING AND CURRENT ROW)
ORDER BY
  epoch_week, day_of_week;
