
DBNAME=$1
DOC=$2


REGION=`grep REGION /a/etc/akamai.conf | awk '{print $1}' | awk -F= '{print $2}'`
NODE=`grep name /a/edgedata/etc/vm.args  | awk '{print $2}'`
COOKIE=`grep setcookie /a/edgedata/etc/vm.args  | awk '{print $2}'`
couch_version=`/usr/local/akamai/bin/dba_cfgparam --param use_couch_version 2>/dev/null` || exit 123

if [[ $couch_version == "2" ]]; then

  ERL=/a/erlang/bin/erl
  EBIN=/a/edgedata2/lib/couchdb/erlang/lib/nsds_db-1.0/ebin/

else

  ERL=/a/erlang_R14/bin/erl
  EBIN=/a/edgedata/lib/nsds_db/ebin/

fi

urldecode() { : "${*//+/ }"; echo -e "${_//%/\\x}"; }

DDOC=$(urldecode "$DOC")

$ERL -name test@localhost -pa $EBIN -proto_dist inet_nsds -setcookie $COOKIE -noshell -eval '
Node = list_to_atom('\"$NODE\"'),
DbName = '\"$DBNAME\"',
Doc = '\"$DDOC\"',
Reply = rpc:call(Node, mem3, shards, [DbName, Doc], 1000),
timer:sleep(2000),
io:fwrite("~n Shards ~p ~n", [Reply]),
init:stop().
'

