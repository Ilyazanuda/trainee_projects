USE movies_database;
INSERT INTO cleared_movies(
    movie_id,
    movie_title,
    movie_year,
    movie_genre
    )
    WITH RECURSIVE
        lnd_movies_without_baddata AS (
            SELECT
                movie_id,
                TRIM(REGEXP_SUBSTR(TRIM(movie_title_year), '(.*)[^(\\d+)]'))    AS movie_title,
                REGEXP_SUBSTR(TRIM(movie_title_year), '(?<=\\()\\d{4}(?=\\)$)') AS movie_year,
                movie_genres
            FROM
                lnd_movies
            WHERE
                TRIM(movie_title_year)    LIKE        '%(____)'
                AND     movie_genres      NOT LIKE    '(%)'
        ),
        filtered_movies AS (
            SELECT
                movie_id,
                movie_title,
                movie_year,
                movie_genres                           AS genres,
                SUBSTRING_INDEX(movie_genres, '|', 1)  AS movie_genre
            FROM
                lnd_movies_without_baddata
            UNION ALL
            SELECT
                movie_id,
                movie_title,
                movie_year,
                SUBSTRING(genres, CHAR_LENGTH(movie_genre) + 2),
                SUBSTRING_INDEX(SUBSTRING(genres, CHAR_LENGTH(movie_genre) + 2), '|', 1)
            FROM
                filtered_movies
            WHERE
                CHAR_LENGTH(genres) > CHAR_LENGTH(movie_genre)
        )
SELECT
    movie_id,
    movie_title,
    movie_year,
    movie_genre
FROM
    filtered_movies;