#!/bin/sh
ab -c 20 -n 10000 -p foo.dat "http://localhost:11211/push/foo"
