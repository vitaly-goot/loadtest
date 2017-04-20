#!/bin/bash

ME="dtop_"$$"@localhost"
REGION=`grep REGION /a/etc/akamai.conf | awk '{print $1}' | awk -F= '{print $2}'`
COOKIE=`grep setcookie /a/edgedata/etc/vm.args  | awk '{print $2}'`
ERL=/a/erlang_R14/bin/erl
EARGS=(
    -name $ME
    -proto_dist inet_nsds
    -pa /usr/local/akamai/edgedata/lib/nsds_db/ebin
    -pa /usr/local/akamai/edgedata/lib/eper-0.51/ebin
    -setcookie $COOKIE
    -boot start_sasl
    -sasl errlog_type error
    -hidden
    $nettick
    +K true
    +A 32
    +P 10000
 )

$ERL ${EARGS[@]}
