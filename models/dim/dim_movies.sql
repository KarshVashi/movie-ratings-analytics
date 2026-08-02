WITH stg_movies AS (
    SELECT * FROM {{ ref('stg_movies') }}
)

SELECT
    movie_id,

    -- MovieLens embeds the release year in the title, e.g. "Toy Story (1995)".
    -- Splitting it out gives a usable year attribute and a title that displays
    -- and joins cleanly. The original string is retained so nothing is lost and
    -- the parse can be checked against it.
    INITCAP(TRIM(REGEXP_REPLACE(title, '\\s*\\(\\d{4}\\)\\s*$', ''))) AS movie_title,
    TRY_TO_NUMBER(REGEXP_SUBSTR(title, '\\((\\d{4})\\)\\s*$', 1, 1, 'e', 1)) AS release_year,
    title AS source_title,

    SPLIT(genres, '|') AS genre_array,
    genres AS genre_list,

    -- MovieLens encodes "unknown" as the literal string '(no genres listed)'
    -- rather than NULL. Flagging it here means downstream models can exclude it
    -- explicitly instead of each one re-deriving the same magic string.
    genres = '(no genres listed)' AS is_unclassified

FROM stg_movies
