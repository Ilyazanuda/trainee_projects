# Get_movies_spark_df_sql
Get_movies_spark_df_sql is app for search movies in that catalog.

This utility is used for searching movies in the database by the following parameters: genre, release year, title matches. You can also set the number of movies to be output. The array will automatically sort movies from newest to latest.
***

# USAGE
* To use variant spark sql or df go to directory spark_sql or spark_df. 
```
usage: get_movies_pyspark [-h] [-re <regexp> [<regexp> ...]] [-yf <fst-year>] [-yt <lst-year>] [-N <amount>] [-g <genre> or <"genre|genre">] [-sr] [-nd]
```

## Options
```
-h, --help                  show this help message and exit
-re <regexp> [<regexp> ...], --regexp <regexp> [<regexp> ...]
                            filter by movie title
-yf <fst-year>, --year_from <fst-year>
                            first year of search
-yt <lst-year>, --year_to <lst-year>
                            last year of search
-N <amount>                 amount of searched movies
-g <genre> or <"genre|genre">, --genres <genre> or <"genre|genre">
                            filter by movie genre, can be multiple through "adventure|crime" with quotes
-sr, --show_result          show result
-nd, --new_data             upload to HDFS new archive from link in bash script
```

## Examples
```
sh get_movies.sh --regexp Term --year_from 1800 --year_to 2000 -N 20 --show_result
sh get_movies.sh -re Love -yf 2000 -N 10 -nd -sr
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