#!/bin/bash

while [ -n "$1" ]
do
case "$1" in
  -N) N="-N $2";;
  --genres|-g) genres="--genres $2";;
  --year_from|-yf) year_from="--year_from $2";;
  --year_to|-yt) year_to="--year_to $2";;
  --regexp|-re) regexp="--regexp $2";;
  --help|-h) help="--help";;
  --show_result|-sr) show_result="--show_result";;
  --new_data|-nd) new_data="--new_data";;
esac
shift
done

if [ $help ]
then
  python3 get_movies.py -h
else
  python3 setup_dataset.py $new_data
  spark-submit get_movies.py $N $genres $year_from $year_to $regexp $show_result
fi