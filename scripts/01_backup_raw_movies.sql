-- Run once, BEFORE 02_simulate_source_changes.sql.
--
-- RAW_MOVIES is the only copy of this source in the warehouse. The snapshot
-- demonstration mutates it, and without this backup those changes cannot be
-- undone short of re-loading the MovieLens CSV.

CREATE TABLE IF NOT EXISTS MOVIELENS.RAW.RAW_MOVIES_BACKUP AS
SELECT * FROM MOVIELENS.RAW.RAW_MOVIES;

-- Expect 27,278.
SELECT COUNT(*) AS backup_row_count FROM MOVIELENS.RAW.RAW_MOVIES_BACKUP;
