#!/bin/bash
cd /usr/local/bin/hadoop-3.2.1/bin
pwd
FILES=/home/pk/data/nyse/NYSE/NYSE_daily_prices_*.csv
for f in $FILES
do
	echo "Processing $f file..."
	hadoop fs -copyFromLocal $f  /Stocks
done

##chmod +x filename.sh
##./filename.sh
