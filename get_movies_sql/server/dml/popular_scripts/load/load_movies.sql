USE movies_database;
LOAD DATA INFILE 'C:/movies_small.csv' INTO table lnd_movies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(movie_id, movie_title_year, movie_genres);