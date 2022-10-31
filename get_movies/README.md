# Get-movies
Get-movies is app for search movies in that catalog.

This utility is used for searching movies in the database by the following parameters: genre, release year, title matches. You can also set the number of movies to be output. The array will automatically rank movies from best to worst.
***

# USAGE
```
get-movies.py [-h] [-regexp <movie-title>] [-year_from <fst-year>] [-year_to <lst-year>] [-N <amount>] [-genres <genre> or <"genre|genre">]
```

## Options
```
-h, --help                                show this help message and exit
-regexp <movie-title>                     filter by movie title
-year_from <fst-year>                     first year of search
-year_to <lst-year>                       last year of search
-N <amount>                               amount of searched movies
-genres <genre> or <"genre|genre">        filter by movie genre, can be multiple through "adventure|crime" with quotes
```

## Examples
```
python get-movies.py -regexp Term -year-from 1800 -year-to 2000 -N 20
```

***

# Installation

This utility use only the standard libraries.


