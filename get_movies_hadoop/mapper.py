import argparse
import re
import sys
import csv


def get_args() -> dict:
    """
    Returns command line args in dict structure
    """
    parser = argparse.ArgumentParser(description='Get-movies-hadoop is app for search movies in that catalog.',
                                     prog='get-movies-hadoop')
    parser.add_argument('-re', '--regexp',
                        type=str,
                        nargs='+',
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
    parser.add_argument('-g', '--genres',
                        type=str,
                        metavar='<genre> or <"genre|genre">',
                        help='filter by movie genre, can be multiple through "adventure|crime" with quotes')
    parser.add_argument('-N',
                        type=int,
                        metavar='<amount>',
                        help='amount of searched movies')
    args = parser.parse_args()
    return {'genres': args.genres,
            'regexp': args.regexp,
            'year_from': args.year_from,
            'year_to': args.year_to}


def mapper(args_dict, line) -> list:
    """
    Returns full valid movies list
    """
    for genre, title, year in filter_movies(args_dict, line):
        yield genre, title, year


def filter_movies(args_dict, line) -> list:
    """
    Processes and returns movies
    """
    try:
        movie_id, movie_title_year, movie_genres = line
        movie_year = int(re.findall(r'\((\d+)\)$', movie_title_year.strip())[-1])
        movie_title = movie_title_year.replace(f'({movie_year})', '').strip()
        if args_dict['year_from'] and movie_year < args_dict['year_from']:
            return
        if args_dict['year_to'] and movie_year > args_dict['year_to']:
            return
        if args_dict['regexp'] and not re.findall(args_dict['regexp'], movie_title):
            return
        if args_dict['genres']:
            for genre in movie_genres.split('|'):
                if genre.lower() in args_dict['genres'].lower():
                    yield genre, movie_title, movie_year
        else:
            if movie_genres != '(no genres listed)':
                for genre in movie_genres.split('|'):
                    yield genre, movie_title, movie_year
    except (ValueError, IndexError):
        pass


def main() -> None:
    """
    Main function receives command line args and stdin text and outputs the processed movie list
    """
    args_dict = get_args()
    if args_dict['regexp']:
        args_dict['regexp'] = ' '.join(args_dict['regexp']).replace(quotes, '')

    for line in csv.reader(sys.stdin, delimiter=comma, quotechar=quotes):
        for genre, title, year in mapper(args_dict, line):
            if delimiter in title or comma in title:
                title = f'{quotes}{title}{quotes}'
            print(genre, title, year, sep=delimiter, file=sys.stdout)


if __name__ == '__main__':
    delimiter, comma, quotes = ';', ',', '"'
    main()
