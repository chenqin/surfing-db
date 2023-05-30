#!/bin/bash

fh -h drsquirrel-pii-dev > "hostfile.txt"
input="hostfile.txt"

while IFS= read -r line
do
  echo "$line slots=15" 
done < "$input"

rm hostfile.txt