import argparse
import sys
import time
from configparser import ConfigParser
from mysql.connector import connect


def get_args() -> dict:
    """
    Returns cmd args in dict structure
    """
    parser = argparse.ArgumentParser(description='Get-movies is app for search movies in that catalog.')
    parser.add_argument('-g', '--genres',
                        type=str,
                        metavar='<genre> or <"genre|genre">',
                        help='filter by movie genre, can be multiple through "adventure|crime" with quotes')
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
    file_config = 'config_client.ini'
    config = ConfigParser()
    config.read(file_config)
    config_db, config_parse = [list() for _ in range(2)]

    for db_parameter_name in config['DB']:
        db_parameter = config['DB'][db_parameter_name]
        config_db.append(int(db_parameter) if db_parameter.isdigit() else db_parameter)
    for parse_parameter_name in config['PARSE']:
        parse_parameter = config['PARSE'][parse_parameter_name]
        config_parse.append(int(parse_parameter) if parse_parameter.isdigit() else parse_parameter)
    return config_db, config_parse


def search_movies(cursor, args) -> None:
    """
    Execute procedure 'filter_movies' with client args
    """
    genres, regexp, year_from, year_to, number, debug = args.values()
    cursor.callproc('filter_movies', (regexp, genres, year_from, year_to, number))


def show_client_request_result(cursor, header, delimiter, comma, batch_size=1000) -> None:
    """
    Show csv format movies array sorted by the rating and genres
    Counter works if N argument is received
    """
    for result in cursor.stored_results():
        movies = result.fetchmany(size=batch_size)
        print(header)
        while len(movies) > 0:
            for movie in movies:
                if delimiter in movie[1] or comma in movie[1]:
                    movie = list(movie)
                    movie[1] = f'"{movie[1]}"'
                try:
                    print(delimiter.join(map(str, movie)))
                except UnicodeEncodeError:
                    print(str((delimiter.join(map(str, movie))).encode('utf-8'))[2:-1])
            movies = result.fetchmany(size=batch_size)


def main() -> None:
    """
    Returns average ratings from ratings data
    """
    start_script = time.time()

    args = get_args()

    config_db, config_parse = get_configs()
    hostname, port, username, password, database_name = config_db
    header, delimiter, comma, batch_size = config_parse

    try:
        with connect(host=hostname,
                     port=port,
                     user=username,
                     password=password,
                     database=database_name) as connection:
            with connection.cursor() as cursor:
                search_movies(cursor, args)
                show_client_request_result(cursor, header, delimiter, comma, batch_size)
                connection.close()
    except Exception as ex:
        print('Error:', ex, file=sys.stderr)

    if args['debug']:
        print(f'Time spent to execution: {time.time() - start_script}', file=sys.stderr)


if __name__ == '__main__':
    main()
