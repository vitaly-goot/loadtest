#!/bin/bash

DBNAME=$1
DOC=$2

urlencode() { python -c "import urllib; print urllib.quote_plus('$1')"; }

DDOC=$(urlencode "$DOC")
echo curl -s http://127.0.0.1:5050/$DBNAME/$DDOC
curl -s http://127.0.0.1:5050/$DBNAME/$DDOC

exit 0
