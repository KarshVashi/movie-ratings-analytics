-- ONE-OFF REPAIR. Run this if an earlier version of
-- 02_simulate_source_changes.sql hard-deleted movieId = 3 from RAW_MOVIES.
--
-- Symptom: `dbt build` fails on
--   relationships_fct_genome_scores_movie_id__movie_id__ref_dim_movies_
-- with exactly 1,128 results. The genome matrix is 10,381 movies x 1,128 tags,
-- so a single missing movie orphans exactly one tag-vocabulary's worth of rows.
-- fct_ratings' relationships test fails for the same reason once it is reached.
--
-- Safe to run more than once -- it inserts only if the row is absent.

INSERT INTO MOVIELENS.RAW.RAW_MOVIES (movieId, title, genres)
SELECT 3, 'Grumpier Old Men (1995)', 'Comedy|Romance'
WHERE NOT EXISTS (
    SELECT 1 FROM MOVIELENS.RAW.RAW_MOVIES WHERE movieId = 3
);

-- Expect 27,278.
SELECT COUNT(*) AS row_count FROM MOVIELENS.RAW.RAW_MOVIES;

-- Expect 0 rows. Confirms every movie referenced by the genome matrix exists.
SELECT DISTINCT g.movieId
FROM MOVIELENS.RAW.RAW_GENOME_SCORES g
LEFT JOIN MOVIELENS.RAW.RAW_MOVIES m ON m.movieId = g.movieId
WHERE m.movieId IS NULL;

-- Note on the snapshot: snap_movies has already recorded movieId 3 as deleted,
-- and that record stays closed. Re-inserting the row opens a NEW version on the
-- next `dbt snapshot`. That is correct SCD Type 2 behaviour, not damage -- the
-- history now says the title was withdrawn and later reinstated, which is what
-- happened.
