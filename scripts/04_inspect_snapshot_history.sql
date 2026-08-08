-- Run after the final `dbt snapshot`. Output of the first query is the
-- screenshot that belongs in the README.

-- --------------------------------------------------------------------------
-- The reclassified and corrected films: one closed version and one open
-- version each, with the change visible between them.
-- --------------------------------------------------------------------------
SELECT
    movie_id,
    title,
    genres,
    dbt_valid_from,
    dbt_valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN 'current' ELSE 'superseded' END AS version_state
FROM MOVIELENS.SNAPSHOTS.SNAP_MOVIES
WHERE movie_id IN (1, 2, 32, 47, 50)
ORDER BY movie_id, dbt_valid_from;

-- --------------------------------------------------------------------------
-- The withdrawn title: one version, closed by hard_deletes='invalidate' rather
-- than superseded by a newer one. dbt_valid_to is set even though no
-- replacement row ever arrived -- which is the whole point of the setting.
-- --------------------------------------------------------------------------
SELECT
    movie_id,
    title,
    genres,
    dbt_valid_from,
    dbt_valid_to
FROM MOVIELENS.SNAPSHOTS.SNAP_MOVIES
WHERE movie_id = 999999;

-- --------------------------------------------------------------------------
-- Version accounting. Every entity should have exactly one open version except
-- the withdrawn title, which should have none.
-- --------------------------------------------------------------------------
SELECT
    COUNT(*)                            AS total_versions,
    COUNT(DISTINCT movie_id)            AS distinct_movies,
    COUNT_IF(dbt_valid_to IS NULL)      AS open_versions,
    COUNT(*) - COUNT(DISTINCT movie_id) AS superseded_versions
FROM MOVIELENS.SNAPSHOTS.SNAP_MOVIES;

-- --------------------------------------------------------------------------
-- The assertion no_overlapping_versions makes, expressed by hand: no entity
-- should ever hold more than one open version. Expect 0 rows.
-- --------------------------------------------------------------------------
SELECT movie_id, COUNT(*) AS open_versions
FROM MOVIELENS.SNAPSHOTS.SNAP_MOVIES
WHERE dbt_valid_to IS NULL
GROUP BY movie_id
HAVING COUNT(*) > 1;
