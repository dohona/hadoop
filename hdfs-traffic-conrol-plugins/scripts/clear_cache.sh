#!/bin/sh
free -m
#flush any filesystem buffers to disk
sync
# dump the cache
echo 3 | sudo tee /proc/sys/vm/drop_caches;

free -m
