from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, FloatType, ShortType
from math import inf
import pyspark.sql.functions as func
import configparser
import argparse
import os


def get_args():
    """
    Returns command line args in dict structure
    """
    parser = argparse.ArgumentParser(description="Shows movies",
                                     prog="get_movies")
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

    if not args['N']:
        args['N'] = inf

    if args['regexp']:
        args['regexp'] = ' '.join(args['regexp'])

    args['genres'] = args['genres'].split('|') if args['genres'] else []

    return args


def get_cfg(cfg_path='config/config.ini'):
    """
    Returns dict with configurations
    """
    cfg = configparser.ConfigParser()
    cfg.read(cfg_path)
    cfg_dict = {}

    for section in cfg.sections():
        cfg_dict.setdefault(section, {})
        cfg_dict[section].update({param: cfg[section][param] for param in cfg[section]})

    return cfg_dict


def get_schemas():
    """
    Returns schema of file
    """
    schema_movies = StructType([StructField('mv_id', IntegerType(), False),
                                StructField('mv_title_year', StringType(), False),
                                StructField('mv_genres', StringType(), False)])

    schema_ratings = StructType([StructField('usr_id', IntegerType(), False),
                                 StructField('mv_id', IntegerType(), False),
                                 StructField('rating', FloatType(), False),
                                 StructField('timestamp', StringType(), False)])
    return schema_movies, schema_ratings


def get_read_csv(spark, schema, path):
    """
    returns read csv in schema form
    """
    return spark.read \
        .format('csv') \
        .option('header', 'true') \
        .option('path', path) \
        .schema(schema) \
        .load()


def write_csv(df, delimiter, path):
    """
    Write movies in csv structure
    """
    df.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .option('delimiter', delimiter) \
        .csv(path)


def normalize_ratings(ratings):
    return ratings.groupBy('mv_id').agg(func.avg('rating').alias('mv_rating'))


def normalize_movies(movies):
    """
    Returns normalized movies
    """
    regexp_title, regexp_year, regexp_bad_genres = r'.+(?=\(\d{4}\))', r'(?<=\()\d{4}(?=\))', r'\(.*\)'
    return movies \
        .filter(~movies.mv_genres.rlike(regexp_bad_genres)) \
        .select('mv_id',
                func.trim(func.regexp_extract('mv_title_year', regexp_title, 0)).alias('mv_title'),
                func.regexp_extract('mv_title_year', regexp_year, 0).cast(ShortType()).alias('mv_year'),
                func.explode(func.split(movies.mv_genres, r'\|')).alias('mv_genre')) \
        .dropna()


def filter_movies(movies, args):
    """
    Returns filtered movies
    """
    if args['regexp']:
        movies = movies.filter(movies.mv_title.rlike(args['regexp']))
    if args['genres']:
        movies = movies.filter(movies.mv_genre.isin(args['genres']))
    if args['year_from']:
        movies = movies.filter(movies.mv_year >= args['year_from'])
    if args['year_to']:
        movies = movies.filter(movies.mv_year <= args['year_to'])
    return movies


def get_result(movies, ratings, n):
    """
    Returns movies result
    """
    movies_ratings = movies.join(ratings, 'mv_id', 'inner')
    n = min(n, movies_ratings.count())
    window_genre = Window.partitionBy('mv_genre').orderBy('mv_genre')

    movies_ratings = movies_ratings.coalesce(1)
    movies_ratings = movies_ratings \
        .orderBy(movies_ratings.mv_genre.asc(),
                 movies_ratings.mv_rating.desc(),
                 movies_ratings.mv_year.desc(),
                 movies_ratings.mv_title.asc()) \
        .withColumn('top_n', func.row_number().over(window_genre))
    return movies_ratings \
        .filter(movies_ratings.top_n <= n) \
        .select(movies_ratings.mv_genre.alias('genre'),
                movies_ratings.mv_title.alias('title'),
                movies_ratings.mv_year.alias('year'),
                func.round('mv_rating', 4).alias('rating'))


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
    spark, args, cfg = SparkSession(SparkContext()), get_args(), get_cfg()

    schema_movies, schema_ratings = get_schemas()

    movies_raw = get_read_csv(spark, schema_movies, cfg['PATHS']['movies'])
    ratings_raw = get_read_csv(spark, schema_ratings, cfg['PATHS']['ratings'])

    ratings_normalized = normalize_ratings(ratings_raw)

    movies_normalized = normalize_movies(movies_raw)
    movies_filtered = filter_movies(movies_normalized, args)

    movies_ratings = get_result(movies_filtered, ratings_normalized, args['N'])

    write_csv(movies_ratings, cfg['PARSE']['delimiter'], cfg['PATHS']['result'])

    spark.stop()
    show_result(args, cfg['PATHS']['result_path'])


if __name__ == '__main__':
    main()
