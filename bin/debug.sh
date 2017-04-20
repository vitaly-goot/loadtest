#!/bin/bash 

usage(){
    cat <<- end
usage: `basename $0` [-setcookie <cookie>] [-nocookie] [-nettick <tick>] 
       [-win] [-dbg] [-vsn <OTP version>] [ -targ node] [-proxy proxynode] 
end
    exit
}

#ERL=erl
START="-s dtop start"

while [ -n "$1" ] 
  do
  case $1 in
      "-help"|"-h"|"-?")
      usage
      ;;
      "-dbg")
      ;;
      "-win")
      WIN="xterm -sb -sl 9999 -bg gold -fg black -e"
      ;;
      "-vsn"|"-version")
      shift
      VSN="+R "$1
      ;;
      "-nettick"|"-tick")
      shift 
      nettick="-kernel net_ticktime $1"
      ;;
      "-cookie"|"-setcookie")
      shift
      cookie="-setcookie $1"
      ;;
      "-nocookie")
      cookie=""
      ;;
      "-proxy")
      shift
      proxy="$1"
      ;;
      "-targ")
      shift
      TARG="$1"
      ;;
      *)
      if [ $# -eq 1 ]; then
          usage
      fi
      ;;
  esac
  shift
done


ME="debug_"$$"@localhost"
REGION=`grep REGION /a/etc/akamai.conf | awk '{print $1}' | awk -F= '{print $2}'`
COOKIE=`grep setcookie /a/edgedata/etc/vm.args  | awk '{print $2}'`
if [ -z "$TARG" ]; then 
    TARG=`grep name /a/edgedata/etc/vm.args  | awk '{print $2}'`
fi
couch_version=`/a/bin/cobra_svcvar -r $REGION nsds::use_couch_version`

START="$START $TARG $proxy"

if [[ $couch_version == "2" ]]; then

  ERL=/a/erlang/bin/erl
  EBIN=/a/edgedata2/lib/couchdb/erlang/lib/nsds_db-1.0/ebin/
  EARGS=(
    -name $ME
    -proto_dist inet_nsds
    -pa /usr/local/akamai/edgedata2/lib/couchdb/erlang/lib/nsds_db-1.0/ebin
    -pa /usr/local/akamai/edgedata2/lib/couchdb/erlang/lib/eper-0.98.0/ebin
    -setcookie $COOKIE
    -boot start_sasl
    -sasl errlog_type error
    -hidden
    $nettick
    +K true
    +A 32
    +P 10000
 )

else

  ERL=/a/erlang_R14/bin/erl
  EARGS=(
    -name $ME
    -proto_dist inet_nsds
    -pa /usr/local/akamai/edgedata/lib/nsds_db/ebin
    -pa /usr/local/akamai/edgedata/lib/eper-0.98.0/ebin
    -setcookie $COOKIE
    -boot start_sasl
    -sasl errlog_type error
    -hidden
    $nettick
    +K true
    +A 32
    +P 10000
 )

fi

$ERL ${EARGS[@]} $START

