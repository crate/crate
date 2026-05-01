#!/bin/bash

for r in $(ls)
do
    version=$(grep -e "^Version" $r | cut -f2 -d" ")
    released=$(grep -e "^Released on" $r | cut -f3 -d" " | tr -d "." | tr "/" "-")
    if [ "$released" != "" ]
    then
        echo "$released $version"
    fi
done
