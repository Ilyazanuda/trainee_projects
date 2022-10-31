USE movies_database;
DROP PROCEDURE IF EXISTS filter_movies;
CREATE PROCEDURE filter_movies(
	IN	arg_regexp		VARCHAR(256),
    IN	arg_genres		VARCHAR(256),
    IN	arg_year_from	SMALLINT,
    IN	arg_year_to		SMALLINT,
    IN	arg_number		SMALLINT
    )
WITH movies_cte AS (
SELECT
	m.movie_id			            AS	movie_id,
	m.movie_genre		            AS	movie_genre,
	m.movie_title		            AS	movie_title,
	m.movie_year		            AS	movie_year,
	r.movie_avg_rating	            AS	movie_avg_rating,
	ROW_NUMBER() OVER(
		PARTITION BY movie_genre
		ORDER BY
			movie_avg_rating        DESC,
			movie_year 			    DESC,
			movie_title 		    ASC
		)                           AS  movie_rank
FROM
	cleared_movies 		m,
	cleared_ratings 	r
WHERE
	m.movie_id = r.movie_id
	AND (arg_year_from	IS NULL
		 OR arg_year_from <= movie_year)
	AND (arg_year_to	IS NULL
		 OR arg_year_to >= movie_year)
	AND (arg_genres     IS NULL
		 OR movie_genre = REGEXP_SUBSTR(arg_genres, movie_genre))
	AND (arg_regexp     IS NULL
		 OR arg_regexp = REGEXP_SUBSTR(movie_title, arg_regexp))
)
SELECT
	movie_genre,
	movie_title,
	movie_year,
	movie_avg_rating
FROM
	movies_cte
WHERE
	arg_number     IS NULL
	OR arg_number >= movie_rank;