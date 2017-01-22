#!/bin/bash

cd ~/SRC/finmonitor
PATH=~/anaconda3/envs/pandasfin/bin:$PATH
export PATH

echo '=============================================='
now=$(date '+%Y-%m-%d %H:%M:%S')
echo $now
python findbupdate.py

