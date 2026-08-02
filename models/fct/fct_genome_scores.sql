-- Materialised as a view, overriding the table default for the fct layer.
--
-- This model applies a ROUND() and nothing else. Making it a table would store a
-- second copy of all 11,709,768 genome rows purely to hold a rounded float --
-- storage bought for no gain, since the rounding is trivial at query time. The
-- expensive work in this branch of the DAG is the flatten-and-aggregate in
-- mart_tag_genre_affinity, and that is where the table lives. The model still
-- earns its place: it declares, documents and tests the grain.
--
-- There is deliberately no `WHERE relevance > 0` here. Checked against the
-- source, that filter would remove zero of 11,709,768 rows (the minimum
-- relevance is 0.00025) -- so it would look like validation while asserting
-- nothing, and would go on looking like validation on the day the property
-- stopped holding. The bound is an explicit range test in the schema file
-- instead, because a test can fail and a no-op filter cannot.
{{ config(materialized = 'view') }}

WITH stg_genome_scores AS (
    SELECT * FROM {{ ref('stg_genome_scores') }}
)

SELECT
    movie_id,
    tag_id,
    ROUND(relevance, 4) AS relevance_score
FROM stg_genome_scores
