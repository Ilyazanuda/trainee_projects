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

#paths
movies_csv=movies_small.csv
movies_csv_path=data/movies_small.csv
get_movies="/get_movies"
hadoop_streaming_jar=$(sudo find / -name "hadoop-streaming.jar" | head -n 1)

hdfs dfs -rm -r $get_movies 2>/dev/null
hdfs dfs -mkdir $get_movies 2>/dev/null
hdfs dfs -put $movies_csv_path $get_movies/$movies_csv 2>/dev/null

chmod +x *.py

if [ $help ]
then
  python3 mapper.py -h
else
  yarn jar $hadoop_streaming_jar \
    -input $get_movies/$movies_csv \
    -output $get_movies/output \
    -file mapper.py \
    -file reducer.py \
    -mapper "python3 mapper.py $genres $year_from $year_to $regexp" \
    -reducer "python3 reducer.py $N"
fi
