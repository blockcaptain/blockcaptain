#!/bin/bash

copy() {
    rsync -v target/debug/blkcaptctl target/debug/blkcaptwrk "blkcaptdev1:"
}

copy
inotifywait -me create target/debug |
    while read path action file; do
        echo $path - $action -  $file
        if [[ $file == "blkcaptctl" || $file == "blkcaptwrk" ]]; then 
            copy
        fi
    done