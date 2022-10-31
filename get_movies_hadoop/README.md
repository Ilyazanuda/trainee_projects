# Get-movies
Get-movies-hadoop is app for search movies in that catalog.

This utility is used for searching movies in the database by the following parameters: genre, release year, title matches. You can also set the number of movies to be output. The array will automatically sort movies from newest to latest.
***

# USAGE
```
get-movies-hadoop [-h] [-regexp <movie-title>] [-year_from <fst-year>] [-year_to <lst-year>] [-N <amount>] [-genres <genre> or <"genre|genre">]
```

## Options
```
-h, --help                                show this help message and exit
--regexp <movie-title>                    filter by movie title
--year_from <fst-year>                    first year of search
--year_to <lst-year>                      last year of search
--N <amount>                              amount of searched movies
--genres <genre> or <"genre|genre">       filter by movie genre, can be multiple through "adventure|crime" with quotes
```

## Examples
```
sh get-movies-hadoop.sh --regexp Term --year-from 1800 --year-to 2000 -N 20
sh get-movies-local.sh -re Term -yf 1800 -yt 2000 -N 20
```

***

# Installation

* Hadoop and spark needs to be installed for the program to work.
* Pyspark should be installed. Write this command in terminal:
```
pip install -r requirements.txt
```
* Permission for bash script files in program directory is required. Write this command in terminal:
```
chmod +x *.sh
```