#/a/bin/python2.7

from socket import error as SocketError
import threading
import hashlib

def _get_payload(ip, doc, size):
    payload = '{"docs":['
    for i in range(size):
        id = urllib.unquote(doc) + "/%d" % i
        payload += '{"_id":"%s","serverid":{"ip":"%s","type":"nsos_ul","start_time":1527444495.9639785},"state":"committed","record_change_time":1527444837.544082,"root":"698338","vn_hash":"mtNYmO_NHpOJlD2XdQZ-jKH229E","payload":{"vn":"698338/DVR/664150/room2_hls_ch3/c9dae3697b1d0c0a162b7f85601cab7d/profile4/backup/m3u8/playlist","version":{"id":[1527444495.953502,"X2RnBQ"],"type":"file","size":717,"hash":"6c6f84678e033917e0d394a96fbcbc19","mtime":1527444495,"user_metadata":{},"user_metadata_ts":1527444837.5439968,"ranges":[{"range":[0,716],"chunks":[{"id":[1527444495.953502,"X2RnBQ"],"status":"active","sn":["nsos",["18673","/mirror/18673.517.1369165830"],"content/698338/c78/03e/2.mtNYmO_NHpOJlD2XdQZ-jKH229EFbTPimG5eX2RnBQVtM-KYbl5fZGcFAAAAAAAAAs1sb4RnjgM5F-DTlKlvvLwZAABA"]}]}]}}}' % (id, ip)
        payload += ','
    return payload.rstrip(',') + ']}'

dbsign = None
lock = threading.Lock()

def load_test_init(ip, args, **params):
    global dbsign 
    dbsign = subprocess.Popen(["/a/bin/dbsign", "--server"], stdout = subprocess.PIPE, stdin = subprocess.PIPE)
    v = dbsign.stdout.readline()
    assert(v == '- OK 1\n')

    state = CouchUtils.db_info(ip, args['bulk_docs'], **params)
    state['suffix'] = ''.join(chr(i) for i in state["shard_suffix"])
    state['shards'] = {}
    state['debug'] = args['bulk_docs']['debug']
    for shard in params['shards']:
        rc, stat = CouchUtils.data_shard_info(ip, shard, state, **params)
        if rc:
            state['shards'][shard] = stat
    return state

def load_test_do_work(ip, doc, args, **params):
    size = args['bulk_docs']['size']
    debug = args['bulk_docs']['debug']

    local = getattr(params['local'], 'local')
    if not local['init']:
        local['init'] = True
        local['conn'] = None
        local['status'] = {}
        local['counter'] = 0 
        local['heartbit'] = int(random.random() * args['bulk_docs']['heartbit'])

    if local['conn'] == None:
        local['conn'] = httplib.HTTPConnection(ip, 5050)

    local['counter'] += 1

    for i in range(params['nrevs']):
        count = i+1
        try:
            if debug: print >>sys.stderr, "%d [%s] POST /%s/_bulk_docs %s" % (time.time(), ip, params['dbname'],doc)

            payload = _get_payload(ip, doc, size)
            if args['bulk_docs'].has_key('dbsign') and args['bulk_docs']['dbsign']:
                hasher = hashlib.sha256()
                hasher.update(payload)
                payload_hash = hasher.hexdigest()

                lock.acquire()
                dbsign.stdin.write("%d signurl db_readwrite POST %s /%s/_bulk_docs\n" % (local['counter'], payload_hash, params['dbname']))
                s = dbsign.stdout.readline()
                lock.release()

                hdnea = s.split()
                assert(int(hdnea[0]) == local['counter'])
                assert(hdnea[1] == 'OK')

                local['conn'].request("POST", "/%s/_bulk_docs" % params['dbname'], payload, {"EdgeAuth":hdnea[2], "Content-Type":"application/json"})
            else:
                local['conn'].request("POST", "/%s/_bulk_docs" % params['dbname'],  payload, {"Content-Type":"application/json"})


            resp = local['conn'].getresponse()
            out = resp.read()
            if local['status'].has_key(resp.status):
                local['status'][resp.status] += 1
            else:   
                local['status'][resp.status] = 1

            if debug: print >>sys.stderr, "%d [%s] %s" % (time.time(), ip, out)

        except httplib.HTTPException as e:
            print >>sys.stderr, "%d [%s] HTTPException %s in %s" % (time.time(), ip, e, doc)
            #traceback.print_exc(file=sys.stderr)
            local['conn'].close()
            time.sleep(random.random())
            local['conn'] = None
            if local['status'].has_key(500):
                local['status'][500] += 1
            else:
                local['status'][500] = 1
            return
        except SocketError as e:
            print >>sys.stderr, "%d [%s] SocketError %s in %s" % (time.time(), ip, e, doc)
            #traceback.print_exc(file=sys.stderr)
            local['conn'].close()
            time.sleep(random.random())
            local['conn'] = None
            if local['status'].has_key(600):
                local['status'][600] += 1
            else:    
                local['status'][600] = 1
            return
        except ValueError as e:    
            print >>sys.stderr, "%d [%s] ValueError %s in %s" % (time.time(), ip, e, out)
            #traceback.print_exc(file=sys.stderr)
            time.sleep(random.random())
            if local['status'].has_key(700):
                local['status'][700] += 1
            else:    
                local['status'][700] = 1
            return
    pass

    if local['counter'] % args['bulk_docs']['heartbit'] == local['heartbit']:
        print >>sys.stderr, "%d [%s] HEARTBIT (%d) %d %s" % (time.time(), ip, local['tid'], local['counter'] * params['nrevs'], str(sorted(local['status'].items())).replace(' ', ''))
        local['status'] = {}
    
def load_test_done(ip, state, args, **params):
    for shard in state['shards']:
        init_stat = state['shards'][shard]
        rc, final_stat = CouchUtils.data_shard_info(ip, shard, state, **params)
        assert(rc)
        info = CouchReporting.data_shard_info_to_str(shard, init_stat, final_stat)
        print "shard %s %s" % (ip, info)
    sys.stderr.flush()        
