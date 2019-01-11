#!/bin/bash

LOG=dups.log

curl -s 127.0.0.1:5050/vgoot -X DELETE
sleep 1
curl -s 127.0.0.1:5050/vgoot -X PUT
sleep 1
/a/sbin/akamai_run restart nsds_db
sleep 1
while curl 127.0.0.1:5050/; [ $? -ne 0 ]; do sleep 1; done
for X in `seq 1 4`; do perl -e 'print "{\"docs\":["; print join(",", map {"{\"_id\":\"./vgoot/${_}\"}"} (10+$ARGV[0]..99+$ARGV[0])); print "]}";' "$X" | curl -v -H 'Content-Type: application/json' -X POST -d @- 127.0.0.1:5050/vgoot/_bulk_docs | cut -b 1-1500 & done ; wait
sleep 1

echo | tee -a $LOG
echo --------------------------- | tee -a $LOG
echo `date "+%Y-%m-%d %H:%M:%S"` | tee -a $LOG
echo | tee -a $LOG
echo "Dups (if any) should  be listed below:" | tee -a $LOG
curl -s 127.0.0.1:5050/vgoot/_all_docs | grep id | sed 's/.*key\":\"\(.*\)\",.*/\1/g'  | sort | uniq -c | awk '{if ($1 != 1) print $0}' | tee -a $LOG
echo End of Dups | tee -a $LOG
echo | tee -a $LOG

echo Btree dump analysis | tee -a $LOG
find /ghostcache/edgedata -name "*vgoot*.couch" | xargs -I [] sh -c 'echo; echo ===========; echo Analysis of []; python couch_dump.py -s []' | tee -a $LOG
echo Have a good day. Relax. | tee -a $LOG

exit 0
