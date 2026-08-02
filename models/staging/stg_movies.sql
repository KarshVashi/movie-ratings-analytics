WITH source AS (
    SELECT * FROM {{ source('movielens', 'raw_movies') }}
)

SELECT
    movieId AS movie_id,
    title,
    genres
FROM source
