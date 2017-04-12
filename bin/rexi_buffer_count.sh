REGION=`grep REGION /a/etc/akamai.conf | awk '{print $1}' | awk -F= '{print $2}'`
NODE=`grep name /a/edgedata/etc/vm.args  | awk '{print $2}'`
COOKIE=`grep setcookie /a/edgedata/etc/vm.args  | awk '{print $2}'`
MAX_COUNT=$1
couch_version=`/a/bin/cobra_svcvar -r $REGION nsds::use_couch_version`

if [[ $couch_version == "2" ]]; then

  ERL=/a/erlang/bin/erl
  EBIN=/a/edgedata2/lib/couchdb/erlang/lib/nsds_db-1.0/ebin/

else

  ERL=/a/erlang_R14/bin/erl
  EBIN=/a/edgedata/lib/nsds_db/ebin/

fi

$ERL -name test@localhost -pa $EBIN -proto_dist inet_nsds -setcookie $COOKIE -noshell -eval '
Node = list_to_atom('\"$NODE\"'),
MaxCount=list_to_atom('\"$MAX_COUNT\"'),
GetBufferCount = fun(Dest) ->
    Server = list_to_atom(lists:concat([rexi_buffer, "_", Dest])),
    try rpc:call(Node, gen_server, call, [Server, get_buffered_count], 1000) of
    0 ->
        ok;
    Count when is_integer(Count) ->
        {Dest, Count};
    Count ->
        {Dest, undef}
    catch Error ->
        {Dest, error}
    end 
end,
GetMaxCount = fun(Dest) ->
    Server = list_to_atom(lists:concat([rexi_buffer, "_", Dest])),
    try rpc:call(Node, gen_server, call, [Server, get_max_count], 1000) of
    0 ->
        ok;
    Count when is_integer(Count) ->
        {Dest, Count};
    Count ->
        {Dest, undef}
    catch Error ->
        {Dest, error}
    end 
end,
Nodes = rpc:call(Node, mem3, nodes, [], 1000),
timer:sleep(1000),
case MaxCount of
max_count ->
    Count = lists:map(GetMaxCount, Nodes),
    io:fwrite("~n Rexi Max Count ~p ~n", [Count]);
_ ->
    Count = lists:map(GetBufferCount, Nodes),
    io:fwrite("~n Rexi Buffer Count ~p ~n", [Count])
end,
init:stop().
'

