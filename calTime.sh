start_time=`date +%s%N|cut -c1-13`
start=`date`
hadoop jar KMeans.jar KMeans.KMeans -D mapred.map.tasks=4 kdd99_10percent output 2 1 0.5 900
end_time=`date +%s%N|cut -c1-13`
end=`date`
running_time=$(echo "scale=3;($end_time-$start_time)/1000" | bc)
echo "The program running time:$running_time seconds"
echo "start: $start"
echo "end: $end"


