#!/bin/sh
wrk -t 4 -c 20 -d 40s -s post.lua "http://localhost:11211/push/foo"
