-- Grain: one row per (movie, calendar day on which it received ratings), with
-- 7- and 28-day rolling engagement measures.
--
-- This is the model where incremental is load-bearing, and it is deliberately
-- built to contrast with fct_ratings.
--
-- fct_ratings uses the simple pattern: everything newer than the high-water
-- mark, nothing else. That works because a rating is a point event -- once
-- written, no later arrival changes it.
--
-- A rolling window is not a point event. A rating landing today changes the
-- 28-day figure for each of the previous 27 days, so a high-water-mark filter
-- would leave those days frozen at whatever value they held when first computed
-- and the recent tail of the series would be permanently, silently wrong.
--
-- So the incremental window here is a LOOKBACK, not a high-water mark: reprocess
-- the trailing window rather than only appending past it, and merge the results
-- over the top. Two distinct lookbacks are needed and they are not the same
-- number:
--
--   REPROCESS_DAYS (28)  how far back output rows are recomputed and merged
--   READ_DAYS      (56)  how far back input rows are read
--
-- READ_DAYS must be at least twice REPROCESS_DAYS, because computing a correct
-- 28-day window for the oldest day being reprocessed requires the 28 days of
-- history sitting behind it. Reading only 28 days would silently truncate the
-- window at the boundary and under-count the oldest rows in every run -- the
-- classic off-by-one-window bug, and the reason this is worth writing out.

{% set reprocess_days = 28 %}
{% set read_days = reprocess_days * 2 %}

{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['movie_id', 'rating_date'],
        on_schema_change = 'fail'
    )
}}

WITH ratings AS (

    SELECT * FROM {{ ref('fct_ratings') }}

    {% if is_incremental() %}
    WHERE rating_timestamp >= (
        SELECT DATEADD(day, -{{ read_days }}, MAX(rating_date))
        FROM {{ this }}
    )
    {% endif %}

),

daily AS (

    SELECT
        movie_id,
        rating_timestamp::DATE AS rating_date,
        COUNT(*)               AS n_ratings,
        AVG(rating)            AS avg_rating
    FROM ratings
    GROUP BY movie_id, rating_timestamp::DATE

),

-- RANGE frames need a numeric ordering column to count calendar days rather
-- than rows. ROWS BETWEEN 6 PRECEDING would mean "the last 7 days on which this
-- film happened to be rated", which for a long-tail catalogue can span years.
-- day_number makes the frame mean what it says.
daily_indexed AS (

    SELECT
        *,
        DATEDIFF('day', '1970-01-01'::DATE, rating_date) AS day_number
    FROM daily

),

rolling AS (

    SELECT
        movie_id,
        rating_date,
        n_ratings,
        avg_rating,

        SUM(n_ratings) OVER (
            PARTITION BY movie_id ORDER BY day_number
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS n_ratings_7d,

        SUM(n_ratings) OVER (
            PARTITION BY movie_id ORDER BY day_number
            RANGE BETWEEN {{ reprocess_days - 1 }} PRECEDING AND CURRENT ROW
        ) AS n_ratings_28d,

        -- Weighted by daily volume, so a day with one 5-star rating does not
        -- carry the same weight as a day with four hundred.
        SUM(n_ratings * avg_rating) OVER (
            PARTITION BY movie_id ORDER BY day_number
            RANGE BETWEEN {{ reprocess_days - 1 }} PRECEDING AND CURRENT ROW
        )
        / NULLIF(
            SUM(n_ratings) OVER (
                PARTITION BY movie_id ORDER BY day_number
                RANGE BETWEEN {{ reprocess_days - 1 }} PRECEDING AND CURRENT ROW
            ), 0
        ) AS avg_rating_28d

    FROM daily_indexed

)

SELECT
    movie_id,
    rating_date,
    n_ratings,
    ROUND(avg_rating, 4)     AS avg_rating,
    n_ratings_7d,
    n_ratings_28d,
    ROUND(avg_rating_28d, 4) AS avg_rating_28d
FROM rolling

{% if is_incremental() %}
-- Emit only the reprocess window. The extra history behind it was read to make
-- the windows correct, not to be written back -- without this filter every run
-- would re-merge READ_DAYS of unchanged rows.
WHERE rating_date >= (
    SELECT DATEADD(day, -{{ reprocess_days }}, MAX(rating_date))
    FROM {{ this }}
)
{% endif %}
