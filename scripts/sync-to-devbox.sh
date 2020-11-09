#!/bin/bash

copy() {
    rsync -v target/debug/$1 "blkcaptdev1:"
}

copy blkcaptctl
copy blkcaptwrk

inotifywait -me create target/debug |
    while read path action file; do
        echo $path - $action -  $file
        if [[ $file == "blkcaptctl" || $file == "blkcaptwrk" ]]; then 
            copy $file
        fi
    done