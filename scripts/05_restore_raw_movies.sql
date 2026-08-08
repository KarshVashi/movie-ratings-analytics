-- Undoes 02_simulate_source_changes.sql. Requires 01_backup_raw_movies.sql to
-- have been run first.
--
-- Note this restores the SOURCE only. The snapshot table keeps its history, as
-- it should -- a snapshot is a record of what the source said at each point in
-- time, and restoring the source does not un-say it. To reset the snapshot as
-- well, drop it and re-run `dbt snapshot`.

BEGIN;

TRUNCATE TABLE MOVIELENS.RAW.RAW_MOVIES;

INSERT INTO MOVIELENS.RAW.RAW_MOVIES
SELECT * FROM MOVIELENS.RAW.RAW_MOVIES_BACKUP;

COMMIT;

-- Expect 27,278.
SELECT COUNT(*) AS restored_row_count FROM MOVIELENS.RAW.RAW_MOVIES;
