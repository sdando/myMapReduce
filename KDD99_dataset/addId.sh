#!/bin/bash
file=$1
awk 'BEGIN {
    FS=","
    line=0
}
{
    print line "," $0
    line=line+1
}' $file
