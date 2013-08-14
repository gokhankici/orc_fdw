#!/bin/bash

protoc-c --c_out=. orc.proto

# Pull the snappy-c submodule
git pull
git submodule init
git submodule update
