USE movies_database;
DROP TABLE IF EXISTS cleared_ratings;
CREATE TABLE cleared_ratings (
    id                  INT                 NOT NULL    AUTO_INCREMENT,
    movie_id            MEDIUMINT           NOT NULL,
    movie_avg_rating    FLOAT,

    CONSTRAINT PK_cleared_ratings PRIMARY KEY (id)
);