#!/bin/sh
wrk -t 4 -c 4 -d 10s -s post.lua "http://localhost:8808/push/foo"
