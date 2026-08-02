-- Grain: one row per (user_id, movie_id). MovieLens permits a user to hold only
-- one rating per film; a re-rating replaces the previous value rather than
-- appending, which is what makes merge the correct strategy here.
--
-- On the materialisation: at 20M rows / ~120MB compressed, a full rebuild of
-- this table takes seconds on an XS warehouse and incremental is NOT justified
-- by volume. It is justified by shape -- this is an append-mostly event stream,
-- which is the case incremental exists for -- and the pattern is kept here
-- deliberately. The point at which it would start paying for itself is roughly
-- two orders of magnitude up from here.
--
-- Two details in the incremental filter are worth spelling out, because the
-- obvious version of each is wrong:
--
--   1. The comparison is `>=`, not `>`. A strict `>` drops any row sharing the
--      exact boundary second with the current maximum. MovieLens timestamps
--      have second granularity: 34.7% of all ratings share their second with
--      another rating, and 14.9% of distinct timestamps are ties, so roughly
--      one boundary in seven would silently lose rows. A bug that needs the
--      right boundary to appear is worse than one that always fires.
--
--   2. `>=` only works paired with unique_key and merge. Alone, it would
--      re-read the boundary second on every run and duplicate it. And without
--      it the model would be append-only: an overlapping re-run duplicates
--      rows, and a corrected rating could never overwrite the original. The two
--      choices are a pair, not two independent decisions.
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['user_id', 'movie_id'],
    on_schema_change = 'fail'
  )
}}

WITH stg_ratings AS (
    SELECT * FROM {{ ref('stg_ratings') }}
)

SELECT
    user_id,
    movie_id,
    rating,
    rating_timestamp
FROM stg_ratings

-- Note there is no `WHERE rating IS NOT NULL` here. It would filter zero of
-- 20,000,263 rows, so it would assert nothing while looking like it did. The
-- not_null test in the schema file states the same property honestly and fails
-- loudly if it ever stops holding, rather than quietly discarding rows.
{% if is_incremental() %}
WHERE rating_timestamp >= (SELECT MAX(rating_timestamp) FROM {{ this }})
{% endif %}
