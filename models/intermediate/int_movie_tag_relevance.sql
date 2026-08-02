-- One row per (movie, genre, genome tag), with the movie's pipe-delimited genre
-- string exploded into one row per genre.
--
-- Ephemeral is the right call here for the reason ephemeral exists: this model
-- has exactly one consumer (mart_tag_genre_affinity), nothing would ever query
-- it directly, and its output is wider and longer than either of its inputs --
-- roughly 11.7M score rows fanned out across ~2 genres per film. Persisting
-- that would mean storing tens of millions of rows so that a single downstream
-- aggregate could scan them once.
--
-- Worth stating the precondition: ephemeral is only defensible while this model
-- has exactly one consumer. An ephemeral model with no consumers never compiles
-- into anything and never runs, and one with several gets its SQL inlined into
-- each of them. If a second mart ever needs this join, it becomes a table.

WITH movies AS (
    SELECT * FROM {{ ref('dim_movies') }}
),

scores AS (
    SELECT * FROM {{ ref('fct_genome_scores') }}
),

tags AS (
    SELECT * FROM {{ ref('dim_genome_tags') }}
),

-- LATERAL FLATTEN turns the genre array into one row per movie-genre pair.
-- Unclassified films are dropped rather than carried as a '(no genres listed)'
-- group: they cannot contribute to a genre-level affinity score, and keeping
-- them would put a meaningless bucket into the mart's output.
movie_genres AS (
    SELECT
        m.movie_id,
        g.value::VARCHAR AS genre
    FROM movies m,
         LATERAL FLATTEN(input => m.genre_array) g
    WHERE NOT m.is_unclassified
)

SELECT
    mg.movie_id,
    mg.genre,
    t.tag_id,
    t.tag_name,
    s.relevance_score
FROM movie_genres mg
INNER JOIN scores s ON s.movie_id = mg.movie_id
INNER JOIN tags   t ON t.tag_id   = s.tag_id
