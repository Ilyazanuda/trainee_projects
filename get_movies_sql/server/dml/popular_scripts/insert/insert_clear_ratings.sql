USE movies_database;
INSERT INTO cleared_ratings(
	movie_id,
    movie_avg_rating
	)
SELECT
	movie_id,
    CAST(ROUND(AVG(movie_rating), 4) AS FLOAT) AS movie_avg_rating
FROM lnd_ratings
GROUP BY movie_id;