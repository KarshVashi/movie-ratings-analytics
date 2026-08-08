-- Stages the kind of source-side change a snapshot exists to capture.
--
-- The MovieLens extract is a static 2015 dump, so left alone it would never
-- produce a second version of any row and snap_movies would demonstrate
-- nothing. These edits are deliberate and reproducible, which is why the script
-- is committed rather than run ad hoc -- a reviewer should be able to see that
-- the source drift was staged rather than wonder why the data is inconsistent.
--
-- Full sequence:
--   1. 01_backup_raw_movies.sql
--   2. dbt snapshot                      -- baseline, 27,278 rows, all open
--   3. this script                       -- two updates and one insert
--   4. dbt snapshot                      -- 5 rows superseded, 1 new entity
--   5. 03_simulate_hard_delete.sql       -- withdraw the inserted title
--   6. dbt snapshot                      -- that entity closed, none open
--   7. 04_inspect_snapshot_history.sql   -- screenshot for the README

-- --------------------------------------------------------------------------
-- 1. Genre reclassification. The most common real change in a film catalogue:
--    a title gains an additional genre after review.
-- --------------------------------------------------------------------------
UPDATE MOVIELENS.RAW.RAW_MOVIES
SET genres = genres || '|Thriller'
WHERE movieId IN (1, 32, 47, 50)
  AND genres NOT LIKE '%Thriller%';

-- --------------------------------------------------------------------------
-- 2. Title correction. MovieLens embeds the release year in the title string,
--    and those years are occasionally wrong and later fixed.
-- --------------------------------------------------------------------------
UPDATE MOVIELENS.RAW.RAW_MOVIES
SET title = REPLACE(title, '(1995)', '(1996)')
WHERE movieId = 2;

-- --------------------------------------------------------------------------
-- 3. A new title enters the catalogue. This exists so that
--    03_simulate_hard_delete.sql has something safe to remove.
--
--    An earlier version of this script hard-deleted movieId = 3 instead, which
--    was wrong: that film carries 1,128 genome-score rows, so removing it left
--    1,128 orphaned rows in fct_genome_scores and failed the relationships test
--    in CI. And it is not fixable by picking a different film -- every one of
--    the 27,278 movies in this dataset is referenced by ratings, tags or the
--    genome matrix, so there is no real title that can be deleted without
--    breaking referential integrity somewhere.
--
--    Inserting a row and then withdrawing it demonstrates the same thing and
--    stays consistent, and it models a real scenario: a title is added to the
--    catalogue and later pulled. The id is far above the source maximum
--    (131,262) so it cannot collide.
-- --------------------------------------------------------------------------
INSERT INTO MOVIELENS.RAW.RAW_MOVIES (movieId, title, genres)
SELECT 999999, 'Withdrawn Title (2026)', 'Drama'
WHERE NOT EXISTS (
    SELECT 1 FROM MOVIELENS.RAW.RAW_MOVIES WHERE movieId = 999999
);

-- Expect 27,279.
SELECT COUNT(*) AS row_count_after_changes FROM MOVIELENS.RAW.RAW_MOVIES;
