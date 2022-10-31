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
-h, --help                                          show this help message and exit
-g, --genres        <genre> or <"genre|genre">      filter by movie genre, can be multiple through "adventure|crime" with quotes
-re, --regexp       <movie-title>                   filter by movie title
-yf, --year_from    <fst-year>                      first year of search
-yt, --year_to      <lst-year>                      last year of search
-N                  <amount>                        amount of searched movies
-d, --debug                                         use option to get feedback about completing
```

## Examples
```
python get-movies.py --regexp love --year-from 1800 --year-to 2000 -N 20
python get-movies.py -r love -yf 1800 -yt 2000 -N 20 > movies.txt
```

***

# Installation

Write this command in terminal:
```
pip install -r requirements.txt
```
For server upload files from path `"server\data"` to directory of mysql server upload. 





