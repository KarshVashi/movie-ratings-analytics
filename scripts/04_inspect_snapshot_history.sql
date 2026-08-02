-- Run after the second `dbt snapshot`. Output of the first query is the
-- screenshot that belongs in the README.

-- --------------------------------------------------------------------------
-- The reclassified films: one closed version and one open version each.
-- --------------------------------------------------------------------------
SELECT
    movie_id,
    title,
    genres,
    dbt_valid_from,
    dbt_valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN 'current' ELSE 'superseded' END AS version_state
FROM MOVIELENS.SNAPSHOTS.SNAP_MOVIES
WHERE movie_id IN (1, 2, 3, 32, 47, 50)
ORDER BY movie_id, dbt_valid_from;

-- --------------------------------------------------------------------------
-- Version counts. Expect 27,278 entities, 27,283 rows, and exactly one open
-- version per entity except movie_id 3, which was hard-deleted and should have
-- none.
-- --------------------------------------------------------------------------
SELECT
    COUNT(*)                                          AS total_versions,
    COUNT(DISTINCT movie_id)                          AS distinct_movies,
    COUNT_IF(dbt_valid_to IS NULL)                    AS open_versions,
    COUNT(*) - COUNT(DISTINCT movie_id)               AS superseded_versions
FROM MOVIELENS.SNAPSHOTS.SNAP_MOVIES;

-- --------------------------------------------------------------------------
-- The hard-deleted film: should have a closed record and no open one.
-- --------------------------------------------------------------------------
SELECT movie_id, title, dbt_valid_from, dbt_valid_to
FROM MOVIELENS.SNAPSHOTS.SNAP_MOVIES
WHERE movie_id = 3;
