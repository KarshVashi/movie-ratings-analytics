-- Withdraws the title inserted by 02_simulate_source_changes.sql, to exercise
-- hard_deletes='invalidate' on the snapshot.
--
-- Run this AFTER a `dbt snapshot` has captured the inserted row, otherwise the
-- entity is created and removed between snapshots and never existed as far as
-- the history is concerned.
--
-- A hard delete is the case a snapshot handles least obviously: the row simply
-- stops appearing in the source, with no tombstone and no flag. Without
-- hard_deletes='invalidate' the snapshot would leave the record open forever,
-- asserting that a withdrawn title is still current. With it, dbt closes
-- dbt_valid_to at the run timestamp.
--
-- Only movieId 999999 is touched. Nothing else in this dataset can safely be
-- hard-deleted -- see the note in 02_simulate_source_changes.sql.

DELETE FROM MOVIELENS.RAW.RAW_MOVIES
WHERE movieId = 999999;

-- Expect 27,278 -- back to the original count, but the snapshot retains the
-- history of a title that existed and was withdrawn.
SELECT COUNT(*) AS row_count_after_delete FROM MOVIELENS.RAW.RAW_MOVIES;
