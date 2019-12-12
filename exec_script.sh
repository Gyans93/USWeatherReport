#!/bin/bash
set -e
mvn package

#spark-submit --class cs236.gprak001.DBMSProj --master local[8] target/DBMSProj-1.0-SNAPSHOT.jar /home/gyan/DBMS_Proj/CS236-Dataset/WeatherStationLocations.csv /home/gyan/DBMS_Proj/CS236-Dataset/2006.txt /home/gyan/DBMS_Proj/CS236-Dataset/2007.txt /home/gyan/DBMS_Proj/CS236-Dataset/2008.txt /home/gyan/DBMS_Proj/CS236-Dataset/2009.txt /home/gyan/DBMS_Proj/task1
spark-submit --class cs236.gprak001.DBMSProj --master local[8] target/DBMSProj-1.0-SNAPSHOT.jar "$1" "$2" "$3" "$4" "$5" "$6" "$7"
