#!/bin/bash

inotifywait -me create target/debug |
    while read path action file; do
        echo $path - $action -  $file
        if [[ $file == "blkcaptctl" ]]; then 
            rsync -v target/debug/blkcaptctl "blkcaptdev1:"
        fi
    done