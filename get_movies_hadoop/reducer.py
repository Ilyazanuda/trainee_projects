import argparse
import sys
import csv


def get_args() -> dict:
    """
    Returns command line args in dict structure
    """
    parser = argparse.ArgumentParser(description='Reducer is script to reduce movies list by the argument N.')
    parser.add_argument('-N',
                        type=int,
                        metavar='<amount>',
                        help='amount of searched movies')
    args = parser.parse_args()
    return {'N': args.N}


def show_filtered_movies(movies, args_dict) -> None:
    """
    Outputs filtered movies
    """
    for genre in sorted(movies.keys()):
        movies[genre].sort(key=lambda point: (-point[year_pose], point[title_pose]))
        counter = 0
        for title, year in movies[genre]:
            if counter == args_dict['N']:
                break
            else:
                if delimiter in title or comma in title:
                    title = f'{quotes}{title}{quotes}'
                print(genre, title, year, sep=delimiter, file=sys.stdout)
                counter += 1


def append_movie(line, movies) -> None:
    """
    Adds movie in dict by genre
    """
    genre, title, year = line
    movies.setdefault(genre, []).append((title, int(year)))


def main() -> None:
    """
    Main function receives command line args and stdin text and runs functions
    """
    args_dict = get_args()
    movies = dict()
    for line in csv.reader(sys.stdin, delimiter=delimiter, quotechar=quotes):
        append_movie(line, movies)
    show_filtered_movies(movies, args_dict)


if __name__ == '__main__':
    title_pose, year_pose = 0, 1
    delimiter, comma, quotes = ';', ',', '"'
    main()
