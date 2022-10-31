USE movies_database;
DROP TABLE IF EXISTS lnd_movies;
CREATE TABLE lnd_movies (
    id                  INT             NOT NULL    AUTO_INCREMENT,
    movie_id            MEDIUMINT       NOT NULL,
    movie_title_year    VARCHAR(255),
    movie_genres        VARCHAR(255),

    CONSTRAINT PK_lnd_movies PRIMARY KEY (id)
);