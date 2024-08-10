/* Fixed */
SELECT
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
END AS holiday_name

/* Floating */
SELECT CASE 
  WHEN month = 1 AND nth_monday_of_month = 3 THEN "Martin Luther King Jr. Day"
  WHEN month = 2 AND nth_monday_of_month = 3 THEN "Presidents' Day"
  WHEN month = 5 AND last_monday_of_month = 1 THEN "Memorial Day"
  WHEN month = 9 AND nth_monday_of_month = 1 THEN "Labor Day"
  WHEN month = 10 AND nth_monday_of_month = 2 THEN "Columbus Day"
  WHEN month = 11 AND nth_thursday_of_month = 4 THEN "Thanksgiving Day"
  WHEN month = 5 AND nth_sunday_of_month = 2 THEN "Mother's Day"
  WHEN month = 6 AND nth_sunday_of_month = 3 THEN "Father's Day"
  WHEN month = 11 AND elapsed_month_thursdays = 4 AND day_of_week = 5 THEN "Black Friday"
  ELSE NULL
END AS holiday_name


/* Announced */
CREATE TEMP FUNCTION is_super_bowl_sunday(calendar_date DATE) AS (
  -- Placeholder for announced date
  FALSE
);



/* TODO these will need to be pulled by ChatGPT most easily. */
CREATE TEMP FUNCTION is_academy_awards(calendar_date DATE) AS (
  -- Placeholder for announced date
  FALSE
);

CREATE TEMP FUNCTION is_nba_finals(calendar_date DATE) AS (
  -- Placeholder for announced date
  FALSE
);

CREATE TEMP FUNCTION is_mlb_world_series(calendar_date DATE) AS (
  -- Placeholder for announced date
  FALSE
);

CREATE TEMP FUNCTION is_major_golf_tournament(calendar_date DATE) AS (
  -- Placeholder for announced date
  FALSE
);

CREATE TEMP FUNCTION is_major_tennis_tournament(calendar_date DATE) AS (
  -- Placeholder for announced date
  FALSE
);

CREATE TEMP FUNCTION is_grammy_awards(calendar_date DATE) AS (
  -- Placeholder for announced date
  FALSE
);

CREATE TEMP FUNCTION is_emmys(calendar_date DATE) AS (
  -- Placeholder for announced date
  FALSE
);
