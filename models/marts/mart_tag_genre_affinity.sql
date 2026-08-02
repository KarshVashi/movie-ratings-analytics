-- Question: which genome tags actually characterise a genre, as opposed to
-- merely being common everywhere?
--
-- Grain: one row per (genre, tag). ~20 genres x 1,128 tags, so roughly 22K rows
-- out of an ~23M-row intermediate join.
--
-- avg_relevance alone answers the wrong question: tags like "Good Acting" score
-- highly across every genre and would top all 20 lists. relevance_lift_vs_all_genres
-- subtracts each tag's own cross-genre mean, so what surfaces is what makes a
-- genre distinctive rather than what is generically popular. That contrast is
-- the point of the model.
--
-- Table, and this is the clearest table justification in the project: expensive
-- input (11.7M relevance rows fanned out across ~2 genres per film, then
-- aggregated with a median), trivial output, read repeatedly. Exactly the shape
-- where paying once to store beats paying every query to recompute.

WITH relevance AS (
    SELECT * FROM {{ ref('int_movie_tag_relevance') }}
),

genre_groups AS (
    SELECT * FROM {{ ref('seed_genre_groups') }}
),

by_genre_tag AS (
    SELECT
        genre,
        tag_id,
        tag_name,
        COUNT(DISTINCT movie_id) AS n_movies,
        AVG(relevance_score)     AS avg_relevance,
        MEDIAN(relevance_score)  AS median_relevance
    FROM relevance
    GROUP BY genre, tag_id, tag_name
),

-- The taxonomy join and the format filter happen BEFORE the cross-genre
-- baseline is computed, not after. IMAX is a delivery format that MovieLens
-- files alongside genres; if it were still present when the baseline is taken,
-- every tag's "mean across all genres" would be partly a mean across a
-- non-genre, and every lift figure would be quietly skewed by it. The seed
-- marks the row explicitly rather than this model hard-coding the string.
genre_scored AS (

    SELECT
        b.genre,
        b.tag_id,
        b.tag_name,
        b.n_movies,
        b.avg_relevance,
        b.median_relevance,
        g.genre_group
    FROM by_genre_tag b
    LEFT JOIN genre_groups g ON g.genre = b.genre
    WHERE NOT COALESCE(g.is_format_not_genre, FALSE)

),

-- The lift is computed in its own CTE rather than inline, because Snowflake
-- will not allow a window function to be nested inside another one -- and the
-- rank below needs to order by this value.
with_lift AS (

    SELECT
        *,
        -- How much more (or less) this tag applies to this genre than to films
        -- in general. Positive means distinctive to the genre.
        avg_relevance - AVG(avg_relevance) OVER (PARTITION BY tag_id)
            AS relevance_lift_vs_all_genres
    FROM genre_scored

)

SELECT
    genre_group,
    genre,
    tag_id,
    tag_name,
    n_movies,

    ROUND(avg_relevance, 4)                AS avg_relevance,
    ROUND(median_relevance, 4)             AS median_relevance,
    ROUND(relevance_lift_vs_all_genres, 4) AS relevance_lift_vs_all_genres,

    RANK() OVER (
        PARTITION BY genre
        ORDER BY relevance_lift_vs_all_genres DESC
    ) AS tag_rank_in_genre

FROM with_lift
