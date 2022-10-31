USE movies_database;
DROP TABLE IF EXISTS lnd_ratings;
CREATE TABLE lnd_ratings (
    id              INT                 NOT NULL    AUTO_INCREMENT,
    user_id         INT                 NOT NULL,
    movie_id        MEDIUMINT           NOT NULL,
    movie_rating    DECIMAL(5, 2),
    timestamp       VARCHAR(255),

    CONSTRAINT PK_lnd_ratings PRIMARY KEY (id)
);