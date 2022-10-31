from pyspark.sql import SparkSession
import configparser
import argparse
import os


def get_args():
    """
    Returns command line args in dict structure
    """
    parser = argparse.ArgumentParser(description='Shows movies',
                                     prog='get_movies')
    parser.add_argument('-re', '--regexp',
                        type=str,
                        metavar='<regexp>',
                        nargs='+',
                        help='filter by movie title')
    parser.add_argument('-yf', '--year_from',
                        type=int,
                        metavar='<fst-year>',
                        help='first year of search')
    parser.add_argument('-yt', '--year_to',
                        type=int,
                        metavar='<lst-year>',
                        help='last year of search')
    parser.add_argument('-N',
                        type=int,
                        metavar='<amount>',
                        help='amount of searched movies')
    parser.add_argument('-g', '--genres',
                        type=str,
                        metavar='<genre> or <"genre|genre">',
                        help='filter by movie genre, can be multiple through "adventure|crime" with quotes')
    parser.add_argument('-sr', '--show_result',
                        action='store_true',
                        help='show result')
    parser.add_argument('-nd', '--new_data',
                        action='store_true',
                        help='upload to HDFS new archive from link in bash script')
    args = vars(parser.parse_args())

    if args['regexp']:
        args['regexp'] = ' '.join(args['regexp'])

    args['genres'] = args['genres'].split('|') if args['genres'] else []

    return args


def get_cfg(cfg_path='config/config.ini'):
    """
    Returns dict with configurations.
    """
    cfg = configparser.ConfigParser()
    cfg.read(cfg_path)
    cfg_dict = {}

    for section in cfg.sections():
        cfg_dict.setdefault(section, {})
        cfg_dict[section].update({param: cfg[section][param] for param in cfg[section]})

    return cfg_dict


def create_lnd_movies(spark, movies_path):
    """
    Creates lnd movies
    """
    spark.sql(
        f"""
        CREATE TEMPORARY VIEW lnd_movies (
            mv_id           INT,
            mv_title_year   STRING,
            mv_genres       STRING
        )
        USING CSV
        OPTIONS (
            HEADER = TRUE,
            PATH = '{movies_path}'
        )
        """
    )


def create_lnd_ratings(spark, ratings_path):
    """
    Creates lnd ratings
    """
    spark.sql(
        f"""
        CREATE TEMPORARY VIEW lnd_ratings (
            usr_id      INT,
            mv_id       INT,
            mv_rating   FLOAT,
            timestamp   STRING
        )
        USING CSV
        OPTIONS (
            HEADER = TRUE,
            PATH = '{ratings_path}'
        )
        """
    )


def create_movies(spark):
    """
    Creates clear movies
    """
    spark.sql(
        """
        CREATE TEMPORARY VIEW movies AS (
        SELECT mv_id,
               REGEXP_EXTRACT(mv_title_year, '(.+)[ ]+[(](\\\d{4})[)]', 1) AS title,
               REGEXP_EXTRACT(mv_title_year, '(.+)[ ]+[(](\\\d{4})[)]', 2) AS year,
               EXPLODE(SPLIT(mv_genres, '[|]')) AS genre
          FROM lnd_movies
         WHERE trim(mv_title_year) LIKE '%(____)'
               AND mv_genres NOT LIKE '(%)'
        )
        """
    )


def create_ratings(spark):
    """
    Creates clear ratings
    """
    spark.sql(
        """
        CREATE TEMPORARY VIEW ratings AS (
            SELECT mv_id, ROUND(AVG(mv_rating), 4) AS rating
              FROM lnd_ratings
             GROUP BY mv_id
        )
        """
    )


def create_movies_ratings(spark):
    """
    Merges movies and ratings
    """
    spark.sql(
        """
        CREATE TEMPORARY VIEW movies_ratings AS (
            SELECT genre, title, year, rating
              FROM movies JOIN ratings ON movies.mv_id = ratings.mv_id
        )
        """
    )


def get_filter_lines(args):
    """
    Returns filters and count_n
    """
    count_n, regexp_filter, genres_filter, year_from_filter, year_to_filter = (None for _ in range(5))

    if args['N']:
        count_n = f'WHERE rank <= {args["N"]}'
    if args['regexp']:
        regexp_filter = f'title RLIKE "{args["regexp"]}"'
    if args['genres']:
        genres = [f'"{genre}"' for genre in args["genres"]]
        genres_filter = f'genre IN ({", ".join(genres)})'
    if args['year_from']:
        year_from_filter = f'year >= {args["year_from"]}'
    if args['year_to']:
        year_to_filter = f'year <= {args["year_to"]}'

    filters = ' AND '.join((item for item in (regexp_filter, genres_filter, year_from_filter, year_to_filter) if item))
    if filters:
        filters = 'WHERE ' + filters
    return filters, count_n


def filter_save_movies_ratings(spark, args, delimiter, path,):
    filters, count_n = get_filter_lines(args)

    spark.sql(
        f"""
        WITH filtered AS (
                SELECT genre, title, year, rating,
                       ROW_NUMBER() OVER (PARTITION BY genre ORDER BY rating DESC, year DESC, title ASC) AS rank
                  FROM movies_ratings
                  {filters}
                  ORDER BY genre ASC, rating DESC, year DESC, title ASC
            )
            SELECT genre, title, year, rating
              FROM filtered
            {count_n}
        """
    ).coalesce(1).write.option('HEADER', 'TRUE').option('DELIMITER', delimiter).csv(path, 'OVERWRITE')


def show_result(args, path):
    """
    Outputs result in terminal
    """
    if args['show_result']:
        print(f'{"-" * 8}RESULT{"-" * 8}')
        os.system(f'hdfs dfs -cat {path}/*')


def main():
    """
    Main function receives arguments and deliver them to other functions
    """
    spark, args, cfg = SparkSession.builder.appName('get_movies').getOrCreate(), get_args(), get_cfg()

    create_lnd_movies(spark, cfg['PATHS']['movies'])
    create_lnd_ratings(spark, cfg['PATHS']['ratings'])

    create_movies(spark)
    create_ratings(spark)

    create_movies_ratings(spark)
    filter_save_movies_ratings(spark, args, cfg['PARSE']['delimiter'], cfg['PATHS']['result'])

    spark.stop()
    show_result(args, cfg['PATHS']['result_path'])


if __name__ == '__main__':
    main()
