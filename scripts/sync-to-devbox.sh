#!/bin/bash

inotifywait -me create target/debug |
    while read path action file; do
        echo $path - $action -  $file
        if [[ $file == "blkcaptctl" || $file == "blkcaptwrk" ]]; then 
            rsync -v target/debug/blkcaptctl target/debug/blkcaptwrk "blkcaptdev1:"
        fi
    done