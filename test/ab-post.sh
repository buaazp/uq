#!/bin/sh
ab -c 20 -n 10000 -p foo.dat "http://localhost:8808/push/foo"
