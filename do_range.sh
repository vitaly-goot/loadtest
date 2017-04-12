#!/bin/bash


PREFIX=$1
FROM=$2
TO=$3
COMMAND=$4
shift 4

# escape the arguments
declare -a args

count=0
for arg in "$@"; do
    args[count]=$(printf '%q' "$arg")
    count=$((count+1))
done


for i in `seq $FROM $TO` 
do
    echo $PREFIX.$i 
    ssh -A root@$PREFIX.$i $COMMAND "${args[@]}"
done
