#!/bin/bash

set -x

mkdir -p "$(dirname "$4")" && touch "$4"
mkdir -p "$(dirname "$5")" && touch "$5"
mkdir -p "$(dirname "$6")" && touch "$6"
mkdir -p "$(dirname "$7")" && touch "$7"

cd Federapp || exit 1
mvn install dependency:copy-dependencies package || exit 1

if [ "$1" -lt "0" ]; then
  echo "Timeout disabled"
  java -classpath "target/Federapp-1.0-SNAPSHOT.jar:target/lib/*" org.example.Federapp "${@:2}"
else
  echo "Timeout enabled"
  timeout --signal=SIGKILL "$1" java -classpath "target/Federapp-1.0-SNAPSHOT.jar:target/lib/*" org.example.Federapp "${@:2}"
fi

EXIT_STATUS=$?
echo "EXIT_STATUS = $EXIT_STATUS"


if [ $EXIT_STATUS -eq 0 ]
then
    echo 'All went fine :)'
    exit 0
fi

if [ $EXIT_STATUS -eq 137 ]
then
    echo 'Process Timed Out!'
    echo "query,exec_time,total_distinct_ss,nb_http_request,total_ss" > "$5"
    echo "$3,failed,failed,failed,failed" >> "$5"
    exit 0
fi

echo "Unknown error :("
echo "query,exec_time,total_distinct_ss,nb_http_request,total_ss" > "$5"
echo "$3,failed,failed,failed,failed" >> "$5"

exit 0