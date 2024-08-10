WITH
  generated_data AS (
  SELECT
    DATE_ADD(DATE '2024-01-01', INTERVAL x DAY) AS ds,
 CASE 
      WHEN x < 20 THEN 100 + RAND() * 2  -- Stable period
      WHEN x BETWEEN 20 AND 30 THEN 200 + RAND() * 5  -- Higher mean, likely to trigger sigma breaches
      WHEN x BETWEEN 31 AND 40 THEN 50 + RAND() * 1  -- Lower mean, likely to trigger low sigma breaches
      WHEN x BETWEEN 41 AND 50 THEN 300 + RAND() * 3  -- Very high values, likely to trigger UCL breaches
      ELSE 10 + RAND() * 2  -- Return to stable
    END AS inputs
  FROM
    UNNEST(GENERATE_ARRAY(0, 99)) AS x
), control_chart_base AS (
  SELECT
    inputs,
    ds,
    FIRST_VALUE(ds) OVER rolling_window AS first_ds_in_window,
    COUNT(ds) OVER rolling_window AS count_dates_in_window,
    AVG(inputs) OVER rolling_window AS average,
    STDDEV(inputs) OVER rolling_window AS standard_deviation,
  FROM
    generated_data
  WINDOW
    rolling_window AS (ORDER BY UNIX_DATE(ds) RANGE BETWEEN 10 PRECEDING AND CURRENT ROW)
), limits AS (
  SELECT
    *,
    average + (1 * standard_deviation) AS ucl_1_sigma,
    average - (1 * standard_deviation) AS lcl_1_sigma,
    average + (2 * standard_deviation) AS ucl_2_sigma,
    average - (2 * standard_deviation) AS lcl_2_sigma,
    average + (3 * standard_deviation) AS ucl_3_sigma,
    average - (3 * standard_deviation) AS lcl_3_sigma,
  FROM
    control_chart_base
), breach_flagging AS (
  SELECT
    *,
	/* TODO refactor for breach counts as columns. */
    (inputs > ucl_3_sigma) AS flag_ucl_breach,
    (inputs < lcl_3_sigma) AS flag_lcl_breach,
    COUNTIF(inputs > ucl_2_sigma) OVER (ORDER BY UNIX_DATE(ds) RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) >= 2 AS flag_2_of_3_above_2s,
    COUNTIF(inputs < lcl_2_sigma) OVER (ORDER BY UNIX_DATE(ds) RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) >= 2 AS flag_2_of_3_below_2s,
    COUNTIF(inputs > ucl_1_sigma) OVER (ORDER BY UNIX_DATE(ds) RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) >= 4 AS flag_4_of_5_above_1s,
    COUNTIF(inputs < lcl_1_sigma) OVER (ORDER BY UNIX_DATE(ds) RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) >= 4 AS flag_4_of_5_below_1s,
    COUNTIF(inputs > average) OVER (ORDER BY UNIX_DATE(ds) RANGE BETWEEN 8 PRECEDING AND CURRENT ROW) >= 8 AS flag_8_above_average,
    COUNTIF(inputs < average) OVER (ORDER BY UNIX_DATE(ds) RANGE BETWEEN 8 PRECEDING AND CURRENT ROW) >= 8 AS flag_8_below_average,
  FROM
    limits
)
SELECT
  *,
  (flag_ucl_breach OR flag_lcl_breach OR flag_2_of_3_above_2s OR flag_2_of_3_below_2s OR flag_4_of_5_above_1s OR flag_4_of_5_below_1s OR flag_8_above_average OR flag_8_below_average)
FROM
  breach_flagging
;

