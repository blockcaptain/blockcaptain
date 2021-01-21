#!/bin/bash

TARGET_HOST="${1:-blkcaptdev1}"

echo "Syncing to $TARGET_HOST..."

copy() {
    rsync -v target/debug/$1 "$TARGET_HOST:/tmp"
    ssh $TARGET_HOST "sudo bash -c \"rm -f '$2'; mkdir -p '$(dirname $2)'; mv '/tmp/$1' '$2'\""
}

COPY_CTL="copy blkcaptctl /usr/bin/blkcapt"
COPY_WRK="copy blkcaptwrk /usr/lib/blockcaptain/blkcaptd"

$COPY_CTL
$COPY_WRK

inotifywait -me create target/debug |
    while read path action file; do
        echo $path - $action -  $file
        if [[ $file == "blkcaptctl" ]]; then 
            $COPY_CTL
        elif [[ $file == "blkcaptwrk" ]]; then 
            $COPY_WRK
        fi
    done