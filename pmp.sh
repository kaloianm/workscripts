#!/bin/bash
#
# Poor man's profiler script (copied verbatim from https://poormansprofiler.org/ and added comments
# for better readability.

# How many times to sample the process
NSAMPLES = 3

# How many seconds to sleep between samples
SLEEP_BETWEEN_SAMPLES_SEC = 0

PID=$(pidof mongos)

for x in $(seq 1 $NSAMPLES)
  do
    gdb -ex "set pagination 0" -ex "thread apply all bt" -batch -p $PID
    sleep $SLEEP_BETWEEN_SAMPLES_SEC
  done | \
awk '
  BEGIN { s = ""; } 
  /^Thread/ { print s; s = ""; } 
  /^\#/ { if (s != "" ) { s = s "," $4} else { s = $4 } } 
  END { print s }' | \
sort | uniq -c | sort -r -n -k 1,1

