-- Question: of the users who first rated in a given month, how many were still
-- rating N months later?
--
-- Grain: one row per (cohort_month, months_since_first_rating). Cohort-by-offset
-- rather than per-user, because the question is about the shape of the decay
-- curve, not about individuals -- and the aggregate is four orders of magnitude
-- smaller than the user-level equivalent.
--
-- A caveat worth stating up front, because it is a property of the data rather
-- than a bug in the model: MovieLens retention collapses almost immediately.
-- Many users arrive having imported a back-catalogue of ratings in a single
-- session, so month 0 is enormous and month 1 is a cliff. That is honest output
-- for this dataset. On a live product the same model would produce the familiar
-- gentle decay, and the SQL would not change.
--
-- Table: cheap to store, expensive to derive (a 20M-row join and two distinct
-- counts), and it is the kind of thing a dashboard hits repeatedly.

WITH users AS (
    SELECT * FROM {{ ref('dim_users') }}
    -- A correctness guard, not a filter. Cohorts are anchored to first rating,
    -- so a user without one would produce a null cohort_month and nonsense
    -- month offsets. It currently removes zero of 138,493 rows, because every
    -- tagging user in MovieLens has also rated -- and that is worth stating
    -- explicitly given this project's own finding that a filter which removes
    -- nothing is usually a filter that should not be there. The difference is
    -- that this one guards a join that would otherwise produce wrong numbers
    -- rather than merely restating an invariant the data already holds.
    WHERE first_rating_at IS NOT NULL
),

ratings AS (
    SELECT * FROM {{ ref('fct_ratings') }}
),

user_month_activity AS (
    SELECT DISTINCT
        u.cohort_month,
        r.user_id,
        DATEDIFF(
            'month',
            u.cohort_month,
            DATE_TRUNC('month', r.rating_timestamp)::DATE
        ) AS months_since_first_rating
    FROM ratings r
    INNER JOIN users u ON u.user_id = r.user_id
),

cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(*) AS cohort_size
    FROM users
    GROUP BY cohort_month
),

active_by_offset AS (
    SELECT
        cohort_month,
        months_since_first_rating,
        COUNT(*) AS active_users
    FROM user_month_activity
    GROUP BY cohort_month, months_since_first_rating
)

SELECT
    a.cohort_month,
    c.cohort_size,
    a.months_since_first_rating,
    a.active_users,
    ROUND(a.active_users / NULLIF(c.cohort_size, 0), 4) AS retention_rate
FROM active_by_offset a
INNER JOIN cohort_sizes c ON c.cohort_month = a.cohort_month
