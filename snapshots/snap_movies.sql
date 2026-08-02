{% snapshot snap_movies %}

-- SCD Type 2 history of movie metadata.
--
-- The obvious thing to snapshot in this dataset is the tag stream, and it is the
-- wrong choice: a tag row is "user U tagged movie M at time T", an immutable
-- event. Events do not slowly change, so there would be no history to capture
-- and every row would sit at version one forever. Movie metadata does change --
-- titles get corrected, genres get reclassified -- so that is what is versioned
-- here.
--
-- Snapshotting the SOURCE, not dim_movies. If a snapshot reads a model, then
-- refactoring that model rewrites history: switching INITCAP to UPPER in
-- dim_movies would record a change to all 27,278 films that never happened
-- upstream. A snapshot has to sit above your own logic or it is versioning your
-- logic rather than the data. The cost of this choice is that pure formatting
-- churn in the source creates versions too -- the right trade, because
-- over-capture is filterable and lost history is not.
--
-- `check` strategy, because raw_movies carries no updated_at column and there is
-- nothing to run a timestamp strategy against. The strategy is forced by the
-- source rather than chosen from preference. check_cols is restricted to the two
-- columns whose changes are meaningful.
--
-- The source query must stay deterministic. Anything that makes the row set vary
-- between runs -- a LIMIT with no ORDER BY, a filter on a volatile column --
-- causes rows to drift in and out of scope, and hard_deletes='invalidate' then
-- closes out records for entities that were never deleted. Nothing in dbt warns
-- about this; the no_overlapping_versions test in _snapshots.yml is what catches
-- it.

{{
    config(
        unique_key='movie_id',
        strategy='check',
        check_cols=['title', 'genres'],
        hard_deletes='invalidate'
    )
}}

SELECT
    movieId AS movie_id,
    title,
    genres
FROM {{ source('movielens', 'raw_movies') }}

{% endsnapshot %}
