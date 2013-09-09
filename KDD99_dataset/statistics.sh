#!/bin/bash
file=$1
awk 'BEGIN {
    FS=","
    normal_count=0
    imnormal_count=0
    smurf_count=0
    line_number=0
}
{
    if ($42=="normal.") normal_count=normal_count+1 
    else imnormal_count=imnormal_count+1
    if($42=="smurf.") smurf_count=smurf_count+1
    line_number=NR
}
END{
printf "The number of normal:%d\n",normal_count
printf "The number of smurf:%d\n",smurf_count
printf "The number of imnormal:%d\n",imnormal_count
printf "The number of record:%d\n",line_number
}' $file

