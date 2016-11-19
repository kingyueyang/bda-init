#!/bin/bash

# NEED DO SOMETHING\
filepath=$(cd "$(dirname "$0")"; pwd)
echo $filepath
docker run --rm --name bda-init -v $filepath/init_zk_data.py:/root/init_zk_data.py:ro bda-init:1.0-2
