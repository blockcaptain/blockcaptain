#!/bin/bash

cargo deb -p blkcaptctl --fast
dpkg -c ./target/debian/blockcaptain_*.deb
dpkg -I ./target/debian/blockcaptain_*.deb
