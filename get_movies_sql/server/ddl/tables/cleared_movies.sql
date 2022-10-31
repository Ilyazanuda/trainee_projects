USE movies_database;
DROP TABLE IF EXISTS cleared_movies;
CREATE TABLE cleared_movies (
    id              INT             NOT NULL    AUTO_INCREMENT,
    movie_id        MEDIUMINT       NOT NULL,
    movie_genre     VARCHAR(255),
    movie_title     VARCHAR(255),
    movie_year      SMALLINT,

    CONSTRAINT PK_cleared_movies PRIMARY KEY (id)
);