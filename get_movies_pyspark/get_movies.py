import pyspark.context
from pyspark import SparkContext
import re
import argparse
import configparser
import os


def get_args():
    """
    Returns cmd args in dict structure
    """
    parser = argparse.ArgumentParser(description='Get_movies_pyspark is app for search movies in that catalog.',
                                     prog='get_movies_pyspark')
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
        args['regexp'] = ' '.join(map(str, args['regexp']))

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


def get_mapped_movies(line: str):
    """
    Returns parsed movie tokens
    """
    try:
        movie_title_year = re.search(r'(?<=,).*(?=,)', line).group(0).strip('"')
        movie_id = int(re.search(r'^\d+', line).group(0))
        movie_genres = re.search(r'[-|\w]+$', line).group(0)
        movie_title = re.search(r'.*(?=\(\d{4}\))', movie_title_year).group(0).strip(' ')
        movie_year = int(re.search(r'(?<=\()\d{4}(?=\))', movie_title_year).group(0))
    except (ValueError, TypeError, AttributeError):
        return ()

    return ((movie_id, (movie_genre, movie_title, movie_year)) for movie_genre in movie_genres.split('|'))


def get_mapped_ratings(line: str):
    """
    Returns parsed ratings tokens
    """
    try:
        _, movie_id, movie_rating, _ = line.split(',')
        movie_id, movie_rating = int(movie_id), float(movie_rating)
    except (ValueError, TypeError, AttributeError):
        return ()

    return movie_id, [movie_rating, 1]


def get_sum_rating(first_tuple_rating: tuple, second_tuple_rating: tuple):
    """
    Returns sum ratings by movie_id and sum count of ratings
    """
    first_rating, first_count = first_tuple_rating
    second_rating, second_count = second_tuple_rating
    return first_rating + second_rating, first_count + second_count


def get_avg_rating(rating_and_count: list):
    """
    Returns average rating by movie_id
    """
    rating, count = rating_and_count
    return round(rating / count, 4)


def filter_movies(movie: tuple, args: dict):
    """
    Filtering movies by args
    """
    movie_id, movie_info = movie
    movie_tokens, movie_rating = movie_info
    movie_genre, movie_title, movie_year = movie_tokens
    if args['year_from'] and movie_year < args['year_from']:
        return
    if args['year_to'] and movie_year > args['year_to']:
        return
    if args['regexp'] and not re.findall(args['regexp'], movie_title):
        return
    if args['genres'] and movie_genre not in args['genres']:
        return
    return True


def sort_movies(movie: tuple):
    """
    Sorting movies by tokens
    """
    movie_id, movie_data = movie
    movie_tokens, movie_rating = movie_data
    movie_genre, movie_title, movie_year = movie_tokens
    return movie_genre, -movie_rating, -movie_year, movie_title


def get_shuffled_movies_tokens(movie: tuple):
    """
    Shuffling movie tokens positions
    """
    movie_id, movie_data = movie
    movie_tokens, movie_rating = movie_data
    movie_genre, movie_title, movie_year = movie_tokens
    return movie_genre, movie_title, movie_year, movie_rating


def get_key_for_group_movies(movie: tuple):
    """
    Returns group token
    """
    movie_genre, movie_title, movie_year, movie_rating = movie
    return movie_genre


def get_reduced_movies_by_count(group_movies_by_genre: tuple, args: dict):
    """
    Returns reduced list by amount argument
    """
    movie_genre, movies = group_movies_by_genre
    return list(movies)[:args['N']]


def get_csv_format(header_rdd: pyspark.context.SparkContext.textFile,
                   movies_result: pyspark.context.SparkContext.textFile,
                   delimiter: str):
    """
    Returns movies in CSV format
    """
    movies_result = movies_result.map(lambda movie_list: delimiter.join(map(str, movie_list)))
    return header_rdd.union(movies_result)


def get_movies(ratings_raw: pyspark.context.SparkContext.textFile,
               movies_raw: pyspark.context.SparkContext.textFile,
               args: dict):
    """
    Returns movies in rdd format
    """
    ratings_normalized = ratings_raw.map(get_mapped_ratings) \
                                    .filter(lambda x: x) \
                                    .reduceByKey(get_sum_rating) \
                                    .mapValues(get_avg_rating)

    movies = movies_raw.flatMap(get_mapped_movies) \
                       .join(ratings_normalized) \
                       .filter(lambda movie: filter_movies(movie, args)) \
                       .sortBy(sort_movies) \
                       .map(get_shuffled_movies_tokens) \
                       .groupBy(get_key_for_group_movies) \
                       .sortByKey() \
                       .flatMap(lambda movies_group: get_reduced_movies_by_count(movies_group, args))

    return movies


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
    args = get_args()
    cfg = get_cfg()
    sc = SparkContext()

    ratings_raw = sc.textFile(cfg['PATHS']['ratings'])
    movies_raw = sc.textFile(cfg['PATHS']['movies'])
    header_rdd = sc.parallelize([cfg['PARSE']['header']])

    movies_result = get_movies(ratings_raw, movies_raw, args)
    get_csv_format(header_rdd, movies_result, cfg['PARSE']['delimiter']).saveAsTextFile(cfg['PATHS']['result'])

    sc.stop()
    show_result(args, cfg['PATHS']['result_path'])


if __name__ == '__main__':
    main()
