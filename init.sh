#!/bin/bash

# Pull the snappy-c submodule
git pull
git submodule init
git submodule update

# Create exec folder
mkdir out
