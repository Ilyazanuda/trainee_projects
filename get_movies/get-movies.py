import argparse
import re
import sys
import csv
import time
from configparser import ConfigParser


def get_args() -> dict:
    """
    Returns cmd args in dict structure
    """
    parser = argparse.ArgumentParser(description='Get-movies is app for search movies in that catalog.')
    parser.add_argument('-re', '--regexp',
                        type=str,
                        metavar='<movie-title>',
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

    debug = parser.add_mutually_exclusive_group()
    debug.add_argument('-d', '--debug',
                       action='store_true',
                       help='use option to get feedback about completing')
    args = parser.parse_args()
    return {'genres': args.genres,
            'regexp': args.regexp,
            'year_from': args.year_from,
            'year_to': args.year_to,
            'N': args.N,
            'debug': args.debug}


def get_configs() -> tuple:
    """
    Returns parsed configs from config.ini
    """
    file_config = 'config.ini'
    config = ConfigParser()
    config.read(file_config)
    config_dirs = [config['PATHS'][parameter] for parameter in config['PATHS']]
    config_parse = [config['PARSE'][parameter] for parameter in config['PARSE']]
    return config_dirs, config_parse


def get_average_ratings(ratings_filename: str) -> dict:
    """
    Returns average ratings from ratings data
    """
    with open(ratings_filename, newline='') as ratings_csv:
        ratings_reader = csv.reader(ratings_csv, delimiter=',', quotechar='"')
        ratings_dict = dict()
        flag_first_line = False
        for data_line in ratings_reader:
            if flag_first_line is False:
                flag_first_line = True
                continue
            try:
                _, movie_id, rating, _ = data_line
            except ValueError:
                continue
            ratings_dict.setdefault(movie_id, [0., 0])
            ratings_dict[movie_id][0] += float(rating)
            ratings_dict[movie_id][1] += 1
        for movie_id, ratings_sum_and_amount in ratings_dict.items():
            ratings_sum, ratings_amount = ratings_sum_and_amount

            ratings_dict[movie_id] = round(ratings_sum / ratings_amount, 4)
    return ratings_dict


def append_matched_movie(movies_items: tuple, movies: dict, args_dict: dict, ratings: dict) -> None:
    """
    Append the correct movies into movies dictionary
    """
    movie_genres, movie_title, movie_year, movie_id = movies_items
    if not ratings.get(movie_id):
        return
    movie_rating = ratings[movie_id]
    if args_dict['year_from'] and movie_year < args_dict['year_from']:
        return
    if args_dict['year_to'] and movie_year > args_dict['year_to']:
        return
    if args_dict['regexp'] and not re.findall(args_dict['regexp'], movie_title):
        return
    if args_dict['genres']:
        for genre in movie_genres.split('|'):
            if genre.lower() in args_dict['genres'].lower():
                movies.setdefault(genre, []).append([movie_title, movie_year, movie_rating])
    else:
        for genre in movie_genres.split('|'):
            movies.setdefault(genre, []).append([movie_title, movie_year, movie_rating])


def get_movies(args_dict: dict, config_paths: list) -> dict:
    """
    Returns sorted movies dictionary by genres with average rating for every movie
    """
    ratings_filename, movies_filename = config_paths
    movies = dict()
    ratings = get_average_ratings(ratings_filename)
    with open(movies_filename, newline='') as movies_csv:
        movies_reader = csv.reader(movies_csv, delimiter=',', quotechar='"')
        flag_first_line = False
        for data_line in movies_reader:
            if flag_first_line is False:
                flag_first_line = True
                continue
            try:
                movie_id, title_with_year, genres = data_line
                year = int(re.findall(r'\((\d+)\)$', title_with_year.strip())[-1])
            except (ValueError, IndexError):
                continue
            if genres == '(no genres listed)':
                continue
            title = title_with_year.replace(f' ({year})', '').rstrip()
            append_matched_movie((genres, title, year, movie_id), movies, args_dict, ratings)
    return movies


def show_sorted_movies(args_dict: dict, config_parse: list, config_paths: list) -> None:
    """
    Show csv format movies array sorted by the rating and genres
    Counter works if N argument is received
    """
    delimiter, comma, header = config_parse
    movies = get_movies(args_dict, config_paths)
    title_pose, year_pose, rating_pose = (0, 1, 2)
    print(header)
    for genre in sorted(movies.keys()):
        movies[genre].sort(key=lambda point: (-point[rating_pose], -point[year_pose], point[title_pose]))
        counter = 0
        for movie_data in movies[genre]:
            if counter == args_dict['N']:
                break
            else:
                if delimiter in movie_data[0] or comma in movie_data[0]:
                    movie_data[0] = f'"{movie_data[0]}"'
                print(f'{genre}{delimiter}{delimiter.join(map(str, movie_data))}')
                counter += 1


def main() -> None:
    """
    Main function receives arguments and convert them into a dictionary
    """
    start = time.time()

    args_dict = get_args()
    config_paths, config_parse = get_configs()

    try:
        show_sorted_movies(args_dict, config_parse, config_paths)
    except Exception as er:
        print(er, file=sys.stderr)

    if args_dict['debug']:
        print(f'Time spent to execution: {time.time() - start} sec.', file=sys.stderr)


if __name__ == '__main__':
    main()
