-- A user dimension with attributes, rather than a bare list of IDs.
--
-- A DISTINCT user_id column would satisfy the relationships tests pointing at
-- this model, and would be a lookup list rather than a dimension -- nothing
-- could be sliced by it. Every attribute below is derived from behaviour the
-- source already records, and cohort_month is what mart_user_cohort_retention
-- groups on, so the dimension has a consumer as well as a shape.

WITH ratings AS (
    SELECT * FROM {{ ref('stg_ratings') }}
),

tags AS (
    SELECT * FROM {{ ref('stg_tags') }}
),

rating_activity AS (
    SELECT
        user_id,
        COUNT(*)              AS n_ratings,
        AVG(rating)           AS avg_rating_given,
        MIN(rating_timestamp) AS first_rating_at,
        MAX(rating_timestamp) AS last_rating_at
    FROM ratings
    GROUP BY user_id
),

tag_activity AS (
    SELECT
        user_id,
        COUNT(*)           AS n_tags,
        MIN(tag_timestamp) AS first_tag_at,
        MAX(tag_timestamp) AS last_tag_at
    FROM tags
    GROUP BY user_id
),

-- Verified against the source: all 7,801 tagging users also appear in ratings,
-- so this UNION currently selects exactly the same 138,493 users that
-- rating_activity alone would. It is kept as a correctness guard rather than a
-- filter -- a tag-only user is possible in the source schema, and taking the
-- ratings stream alone would drop them silently rather than loudly. The guard
-- costs one pass over a 7,801-row aggregate.
all_users AS (
    SELECT user_id FROM rating_activity
    UNION
    SELECT user_id FROM tag_activity
)

SELECT
    u.user_id,

    COALESCE(r.n_ratings, 0) AS n_ratings,
    COALESCE(t.n_tags, 0)    AS n_tags,
    r.avg_rating_given,

    r.first_rating_at,
    r.last_rating_at,
    t.first_tag_at,
    t.last_tag_at,

    -- Cohort is anchored to first rating, not to first activity of any kind.
    -- Rating is the core action of the product; tagging is a secondary behaviour
    -- only a minority of users ever perform, so anchoring cohorts to it would
    -- put a small, unrepresentative group into cohorts of their own.
    DATE_TRUNC('month', r.first_rating_at)::DATE         AS cohort_month,
    DATEDIFF('day', r.first_rating_at, r.last_rating_at) AS active_span_days,

    COALESCE(t.n_tags, 0) > 0 AS has_tagged

-- An is_tag_only_user flag was drafted here and removed: it is false for every
-- row in this dataset, and a column that is constant by construction is noise
-- in a dimension. The UNION above still handles the case; it just does not need
-- advertising as an attribute.
FROM all_users u
LEFT JOIN rating_activity r ON u.user_id = r.user_id
LEFT JOIN tag_activity    t ON u.user_id = t.user_id
