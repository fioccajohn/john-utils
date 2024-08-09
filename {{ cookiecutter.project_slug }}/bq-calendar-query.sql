WITH
preprocessing AS (
  SELECT
    calendar_date,
    EXTRACT(DAYOFWEEK FROM calendar_date) AS weekday,
    EXTRACT(DAY FROM calendar_date) AS day_of_month,
    EXTRACT(DAYOFYEAR FROM calendar_date) AS day_of_year,
    EXTRACT(WEEK FROM calendar_date) AS week,
    EXTRACT(MONTH FROM calendar_date) AS month,
    EXTRACT(QUARTER FROM calendar_date) AS quarter,
    EXTRACT(YEAR FROM calendar_date) AS year,
    EXTRACT(WEEK(SATURDAY) FROM calendar_date) AS retail_week,
    EXTRACT(ISOYEAR FROM calendar_date) AS isoyear,
    EXTRACT(ISOWEEK FROM calendar_date) AS isoweek,
    DATE_TRUNC(calendar_date, WEEK) AS first_day_of_week,
    DATE_TRUNC(calendar_date, MONTH) AS first_day_of_month,
    DATE_TRUNC(calendar_date, QUARTER) AS first_day_of_quarter,
    DATE_TRUNC(calendar_date, YEAR) AS first_day_of_year,
    LAST_DAY(calendar_date, WEEK) AS last_day_of_week,
    LAST_DAY(calendar_date, MONTH) AS last_day_of_month,
    LAST_DAY(calendar_date, QUARTER) AS last_day_of_quarter,
    LAST_DAY(calendar_date, YEAR) AS last_day_of_year,
  FROM
    UNNEST(GENERATE_DATE_ARRAY('1995-10-03', '2096-10-03')) AS calendar_date
), feature_engineering AS (
  SELECT
    *,
	ROW_NUMBER() OVER window__quarter AS day_of_quarter,
	7 AS days_in_week,
    EXTRACT(DAY FROM last_day_of_month) AS days_in_month,
    (DATE_DIFF(last_day_of_quarter, first_day_of_quarter, DAY) + 1) AS days_in_quarter,
    (DATE_DIFF(last_day_of_year, first_day_of_year, DAY) + 1) AS days_in_year,
	(weekday = 1) AS is_sunday,
    (weekday = 3) AS is_tuesday,
    (weekday = 2) AS is_monday,
    (weekday = 4) AS is_wednesday,
    (weekday = 5) AS is_thursday,
    (weekday = 6) AS is_friday,
    (weekday = 7) AS is_saturday,
    (weekday BETWEEN 2 AND 6) AS is_weekday,
    (weekday NOT BETWEEN 2 AND 6) AS is_weekend,
  FROM
    preprocessing
  WINDOW
	window__quarter AS (PARTITION BY quarter)
), postprocessing AS (
  SELECT
    *,
	(calendar_date = first_day_of_week) AS is_first_day_of_week,
	(calendar_date = first_day_of_month) AS is_first_day_of_month,
	(calendar_date = first_day_of_quarter) AS is_first_day_of_quarter,
	(calendar_date = first_day_of_year) AS is_first_day_of_year,
	(calendar_date = last_day_of_week) AS is_last_day_of_week,
	(calendar_date = last_day_of_month) AS is_last_day_of_month,
	(calendar_date = last_day_of_quarter) AS is_last_day_of_quarter,
	(calendar_date = last_day_of_year) AS is_last_day_of_year,
	weekday / days_in_week AS pct_week_elapsed,
	day_of_month / days_in_month AS pct_month_elapsed,
	day_of_quarter / days_in_quarter AS pct_quarter_elapsed,
	day_of_year / days_in_year AS pct_year_elapsed,
	days_in_week / weekday AS simple_scaling_factor__end_of_week,
	days_in_month / day_of_month AS simple_scaling_factor__end_of_month,
	days_in_quarter / day_of_quarter AS simple_scaling_factor__end_of_quarter,
	days_in_year / day_of_year AS simple_scaling_factor__end_of_year,
  FROM
    feature_engineering
)
SELECT
	*
FROM
	postprocessing
;
