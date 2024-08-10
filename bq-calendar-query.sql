WITH
preprocessing AS (
  SELECT
    calendar_date,
    EXTRACT(DAYOFWEEK FROM calendar_date) AS weekday,
    EXTRACT(DAY FROM calendar_date) AS day_of_month,
    EXTRACT(DAYOFYEAR FROM calendar_date) AS day_of_year,
  	CEIL(EXTRACT(DAY FROM calendar_date) / 7) AS week_of_month,
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
), feature_engineering1 AS (
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
	window__quarter AS (PARTITION BY year, quarter)
), feature_engineering2 AS (
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
    COUNTIF(is_sunday) OVER window__month AS elapsed_month_sundays,
    COUNTIF(is_monday) OVER window__month AS elapsed_month_mondays,
    COUNTIF(is_tuesday) OVER window__month AS elapsed_month_tuesdays,
    COUNTIF(is_wednesday) OVER window__month AS elapsed_month_wednesdays,
    COUNTIF(is_thursday) OVER window__month AS elapsed_month_thursdays,
    COUNTIF(is_friday) OVER window__month AS elapsed_month_fridays,
    COUNTIF(is_saturday) OVER window__month AS elapsed_month_saturdays,
    COUNTIF(is_sunday) OVER window__month__boundless AS sundays_in_month,
    COUNTIF(is_monday) OVER window__month__boundless AS mondays_in_month,
    COUNTIF(is_tuesday) OVER window__month__boundless AS tuesdays_in_month,
    COUNTIF(is_wednesday) OVER window__month__boundless AS wednesdays_in_month,
    COUNTIF(is_thursday) OVER window__month__boundless AS thursdays_in_month,
    COUNTIF(is_friday) OVER window__month__boundless AS fridays_in_month,
    COUNTIF(is_saturday) OVER window__month__boundless AS saturdays_in_month,
  FROM
    feature_engineering1
  WINDOW
  	window__month AS (PARTITION BY year, month ORDER BY calendar_date),
  	window__month__boundless AS (PARTITION BY year, month)
), feature_engineering3 AS (
  SELECT
    *,
    IF(is_sunday, elapsed_month_sundays, NULL) AS nth_sunday_of_month,
    IF(is_monday, elapsed_month_mondays, NULL) AS nth_monday_of_month,
    IF(is_tuesday, elapsed_month_tuesdays, NULL) AS nth_tuesday_of_month,
    IF(is_wednesday, elapsed_month_wednesdays, NULL) AS nth_wednesday_of_month,
    IF(is_thursday, elapsed_month_thursdays, NULL) AS nth_thursday_of_month,
    IF(is_friday, elapsed_month_fridays, NULL) AS nth_friday_of_month,
    IF(is_saturday, elapsed_month_saturdays, NULL) AS nth_saturday_of_month,
  FROM
	feature_engineering2
), feature_engineering4 AS (
  SELECT
    *,
	/* TODO should be ecapsulated as functions. */
    /* Fixed */
    CASE FORMAT_DATE('%m-%d', calendar_date)
      WHEN "01-01" THEN "New Year's Day"
      WHEN "07-04" THEN "Independence Day"
      WHEN "11-11" THEN "Veterans Day"
      WHEN "12-25" THEN "Christmas Day"
      WHEN "02-14" THEN "Valentine's Day"
      WHEN "10-31" THEN "Halloween"
      WHEN "03-17" THEN "St. Patrick's Day"
      WHEN "02-02" THEN "Groundhog Day"
      WHEN "06-19" THEN "Juneteenth"
      WHEN "12-31" THEN "New Year's Eve"
      ELSE NULL
    END AS fixed_holidays,
    /* Floating */
    CASE 
      WHEN month = 1 AND nth_monday_of_month = 3 THEN "Martin Luther King Jr. Day"
      WHEN month = 2 AND nth_monday_of_month = 3 THEN "Presidents' Day"
      WHEN month = 5 AND nth_monday_of_month = mondays_in_month THEN "Memorial Day"
      WHEN month = 9 AND nth_monday_of_month = 1 THEN "Labor Day"
      WHEN month = 10 AND nth_monday_of_month = 2 THEN "Columbus Day"
      WHEN month = 11 AND nth_thursday_of_month = 4 THEN "Thanksgiving Day"
      WHEN month = 5 AND nth_sunday_of_month = 2 THEN "Mother's Day"
      WHEN month = 6 AND nth_sunday_of_month = 3 THEN "Father's Day"
      WHEN month = 11 AND elapsed_month_thursdays = 4 AND weekday = 5 THEN "Black Friday"
      ELSE NULL
    END AS floating_holidays,
  FROM
    feature_engineering3
)
SELECT
	*,
	/* TODO should be an array agg but even better should be a list that we join to. */
	COALESCE(fixed_holidays, floating_holidays) AS holiday,
FROM
	feature_engineering4
	/*
GROUP BY
  ALL
*/
;



