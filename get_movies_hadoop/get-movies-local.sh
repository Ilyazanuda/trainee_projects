#!/bin/bash

while [ -n "$1" ]
do
case "$1" in

-N) N="-N $2"
shift ;;

--genres|-g) genres="--genres \"$2\""
shift ;;

--year_from|-yf) year_from="--year_from $2"
shift ;;

--year_to|-yt) year_to="--year_to $2"
shift ;;

--regexp|-re) regexp="--regexp $2"
shift ;;

--help|-h) help="--help"
shift ;;

*) echo "$1 is not an option" 2>/dev/null
exit ;;
esac
shift
done

if [ $help ]
then
	python3 mapper.py -h
else
	cat data/movies_small.csv | python3 mapper.py $genres $year_from $year_to $regexp | sort | python3 reducer.py $N
fi
