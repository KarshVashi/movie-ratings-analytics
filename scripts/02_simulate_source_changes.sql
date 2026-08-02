-- Simulates the kind of source-side change a snapshot exists to capture.
--
-- The MovieLens extract is a static 2015 dump, so left alone it would never
-- produce a second version of any row and snap_movies would demonstrate
-- nothing. These edits are deliberate and reproducible, which is why the script
-- is committed rather than run ad hoc -- a reviewer should be able to see that
-- the source drift was staged rather than wonder why the data is inconsistent.
--
-- Sequence:
--   1. dbt snapshot                        -- baseline, 27,278 rows, all open
--   2. run this script
--   3. dbt snapshot                        -- 5 rows closed, 5 new versions, 1 invalidated
--   4. run 04_inspect_snapshot_history.sql -- screenshot for the README

-- --------------------------------------------------------------------------
-- 1. Genre reclassification. The most common real change in a film catalogue:
--    a title gets an additional genre after review.
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
-- 3. Hard delete, to exercise hard_deletes='invalidate'. The row disappears
--    from the source with no tombstone -- the snapshot should close the record
--    rather than leave it open forever.
-- --------------------------------------------------------------------------
DELETE FROM MOVIELENS.RAW.RAW_MOVIES
WHERE movieId = 3;

-- Expect 27,277.
SELECT COUNT(*) AS row_count_after_changes FROM MOVIELENS.RAW.RAW_MOVIES;
