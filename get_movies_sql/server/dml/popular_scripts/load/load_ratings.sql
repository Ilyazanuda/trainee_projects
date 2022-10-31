USE movies_database;
LOAD DATA INFILE 'C:/ratings_small.csv' INTO TABLE lnd_ratings
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(user_id, movie_id, movie_rating, timestamp);