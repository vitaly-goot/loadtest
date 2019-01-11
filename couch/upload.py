#/a/bin/python2.7

from socket import error as SocketError
import threading
import hashlib

def _get_payload(count,delay,rev):
    #count = 6
    if count == 1 and rev == None:
        # repl,fetch,del,del
        if delay: time.sleep(random.random())
        return "{\"count\":%d,\"root\":\"275016\",\"vn_hash\":\"kCzTwvEwqQpoPRFFwlK4MJqkuW8\",\"aversion\":{\"id\":[1479518055.304166,\"AhAylw\"],\"type\":\"file\",\"size\":1091536,\"hash\":\"dfe0aa7992f95f2d4731ddeb65319780\",\"mtime\":1479518055,\"user_metadata\":{},\"user_metadata_ts\":1479518055.829717,\"ranges\":[{\"range\":[0,1091535],\"chunks\":[{\"id\":[1479518055.304166,\"AhAylw\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19269\",\"/stripe/19269.144.1372878600.8.2\"],\"content/275016/678/c99/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSQgC-YCEDKXAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA1QBA\"]},{\"id\":[1479518070.897756,\"F0_2jQ\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19455\",\"/stripe/19455.283.1375200265.8.2\"],\"content/275016/ea5/3c3/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUN_FwXT_aNAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA6gBA\"]}],\"dcids\":[[1479518070.883097,\"F0_2mw\"]]}]},\"queue\":{\"fetch\":[[[\"nsds\",[\"19461\",\"0\",\"0\"],\"\"],0,1479517259.411536]],\"repl\":[[-1,\"\",0,1479518126.7400022]],\"del\":[[[\"nsos\",[\"19455\",\"/stripe/19455.200.1375200265.8.2\"],\"content/275016/1c0/e0d/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUNwxkXT_abAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeAogBA\"],0,1479518126.740001],[[\"nsos\",[\"19455\",\"/stripe/19455.200.1375200265.8.2\"],\"content/275016/1c0/e0d/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUNwxkXT_abAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeAogBA\"],0,1479518126.740001]]},\"ctime\":1479518067.042391}" % count

    elif count == 1: 
        # repl,fetch,del,del
        if delay: time.sleep(random.random())
        return "{\"_rev\":\"%s\",\"count\":%d,\"root\":\"275016\",\"vn_hash\":\"kCzTwvEwqQpoPRFFwlK4MJqkuW8\",\"aversion\":{\"id\":[1479518055.304166,\"AhAylw\"],\"type\":\"file\",\"size\":1091536,\"hash\":\"dfe0aa7992f95f2d4731ddeb65319780\",\"mtime\":1479518055,\"user_metadata\":{},\"user_metadata_ts\":1479518055.829717,\"ranges\":[{\"range\":[0,1091535],\"chunks\":[{\"id\":[1479518055.304166,\"AhAylw\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19269\",\"/stripe/19269.144.1372878600.8.2\"],\"content/275016/678/c99/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSQgC-YCEDKXAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA1QBA\"]},{\"id\":[1479518070.897756,\"F0_2jQ\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19455\",\"/stripe/19455.283.1375200265.8.2\"],\"content/275016/ea5/3c3/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUN_FwXT_aNAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA6gBA\"]}],\"dcids\":[[1479518070.883097,\"F0_2mw\"]]}]},\"queue\":{\"fetch\":[[[\"nsds\",[\"19461\",\"0\",\"0\"],\"\"],0,1479517259.411536]],\"repl\":[[-1,\"\",0,1479518126.7400022]],\"del\":[[[\"nsos\",[\"19455\",\"/stripe/19455.200.1375200265.8.2\"],\"content/275016/1c0/e0d/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUNwxkXT_abAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeAogBA\"],0,1479518126.740001],[[\"nsos\",[\"19455\",\"/stripe/19455.200.1375200265.8.2\"],\"content/275016/1c0/e0d/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUNwxkXT_abAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeAogBA\"],0,1479518126.740001]]},\"ctime\":1479518067.042391}" % (rev, count)

    elif count == 2:
        # fetch,del,del
        if delay: time.sleep(random.random()*6)
        return "{\"_rev\":\"%s\",\"count\":%d,\"root\":\"275016\",\"vn_hash\":\"kCzTwvEwqQpoPRFFwlK4MJqkuW8\",\"aversion\":{\"id\":[1479518055.304166,\"AhAylw\"],\"type\":\"file\",\"size\":1091536,\"hash\":\"dfe0aa7992f95f2d4731ddeb65319780\",\"mtime\":1479518055,\"user_metadata\":{},\"user_metadata_ts\":1479518055.829717,\"ranges\":[{\"range\":[0,1091535],\"chunks\":[{\"id\":[1479518055.304166,\"AhAylw\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19269\",\"/stripe/19269.144.1372878600.8.2\"],\"content/275016/678/c99/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSQgC-YCEDKXAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA1QBA\"]},{\"id\":[1479518070.897756,\"F0_2jQ\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19455\",\"/stripe/19455.283.1375200265.8.2\"],\"content/275016/ea5/3c3/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUN_FwXT_aNAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA6gBA\"]}],\"dcids\":[[1479518070.883097,\"F0_2mw\"]]}]},\"queue\":{\"fetch\":[[[\"nsds\",[\"19461\",\"0\",\"0\"],\"\"],0,1479517259.411536]],\"del\":[[[\"nsos\",[\"19455\",\"/stripe/19455.200.1375200265.8.2\"],\"content/275016/1c0/e0d/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUNwxkXT_abAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeAogBA\"],0,1479518126.740001],[[\"nsos\",[\"19455\",\"/stripe/19455.200.1375200265.8.2\"],\"content/275016/1c0/e0d/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUNwxkXT_abAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeAogBA\"],0,1479518126.740001]]},\"ctime\":1479518067.042391}" % (rev, count)

    elif count == 3:
        # fetch,del
        if delay: time.sleep(random.random())
        return "{\"_rev\":\"%s\",\"count\":%d,\"root\":\"275016\",\"vn_hash\":\"kCzTwvEwqQpoPRFFwlK4MJqkuW8\",\"aversion\":{\"id\":[1479518055.304166,\"AhAylw\"],\"type\":\"file\",\"size\":1091536,\"hash\":\"dfe0aa7992f95f2d4731ddeb65319780\",\"mtime\":1479518055,\"user_metadata\":{},\"user_metadata_ts\":1479518055.829717,\"ranges\":[{\"range\":[0,1091535],\"chunks\":[{\"id\":[1479518055.304166,\"AhAylw\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19269\",\"/stripe/19269.144.1372878600.8.2\"],\"content/275016/678/c99/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSQgC-YCEDKXAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA1QBA\"]},{\"id\":[1479518070.897756,\"F0_2jQ\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19455\",\"/stripe/19455.283.1375200265.8.2\"],\"content/275016/ea5/3c3/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUN_FwXT_aNAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA6gBA\"]}],\"dcids\":[[1479518070.883097,\"F0_2mw\"]]}]},\"queue\":{\"fetch\":[[[\"nsds\",[\"19461\",\"0\",\"0\"],\"\"],0,1479517259.411536]],\"del\":[[[\"nsos\",[\"19455\",\"/stripe/19455.200.1375200265.8.2\"],\"content/275016/1c0/e0d/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUNwxkXT_abAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeAogBA\"],0,1479518126.740001]]},\"ctime\":1479518067.042391}" % (rev, count)

    elif count == 4:
        # fetch
        if delay: time.sleep(random.random())
        return "{\"_rev\":\"%s\",\"count\":%d,\"root\":\"275016\",\"vn_hash\":\"kCzTwvEwqQpoPRFFwlK4MJqkuW8\",\"aversion\":{\"id\":[1479518055.304166,\"AhAylw\"],\"type\":\"file\",\"size\":1091536,\"hash\":\"dfe0aa7992f95f2d4731ddeb65319780\",\"mtime\":1479518055,\"user_metadata\":{},\"user_metadata_ts\":1479518055.829717,\"ranges\":[{\"range\":[0,1091535],\"chunks\":[{\"id\":[1479518055.304166,\"AhAylw\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19269\",\"/stripe/19269.144.1372878600.8.2\"],\"content/275016/678/c99/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSQgC-YCEDKXAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA1QBA\"]},{\"id\":[1479518070.897756,\"F0_2jQ\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19455\",\"/stripe/19455.283.1375200265.8.2\"],\"content/275016/ea5/3c3/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUN_FwXT_aNAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA6gBA\"]}],\"dcids\":[[1479518070.883097,\"F0_2mw\"]]}]},\"queue\":{\"fetch\":[[[\"nsds\",[\"19461\",\"0\",\"0\"],\"\"],0,1479517259.411536]]},\"ctime\":1479518067.042391}" % (rev, count)

    elif count == 5:
        # repl
        return "{\"_rev\":\"%s\",\"count\":%d,\"root\":\"275016\",\"vn_hash\":\"kCzTwvEwqQpoPRFFwlK4MJqkuW8\",\"aversion\":{\"id\":[1479518055.304166,\"AhAylw\"],\"type\":\"file\",\"size\":1091536,\"hash\":\"dfe0aa7992f95f2d4731ddeb65319780\",\"mtime\":1479518055,\"user_metadata\":{},\"user_metadata_ts\":1479518055.829717,\"ranges\":[{\"range\":[0,1091535],\"chunks\":[{\"id\":[1479518055.304166,\"AhAylw\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19269\",\"/stripe/19269.144.1372878600.8.2\"],\"content/275016/678/c99/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSQgC-YCEDKXAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA1QBA\"]},{\"id\":[1479518070.897756,\"F0_2jQ\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19455\",\"/stripe/19455.283.1375200265.8.2\"],\"content/275016/ea5/3c3/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUN_FwXT_aNAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA6gBA\"]}],\"dcids\":[[1479518070.883097,\"F0_2mw\"]]}]},\"queue\":{\"repl\":[[-1,\"\",0,1479518126.7400022]]},\"ctime\":1479518067.042391}" % (rev, count)

    elif count == 6:
        if delay: time.sleep(random.random())
        # {}
        return "{\"_rev\":\"%s\",\"count\":%d,\"root\":\"275016\",\"vn_hash\":\"kCzTwvEwqQpoPRFFwlK4MJqkuW8\",\"aversion\":{\"id\":[1479518055.304166,\"AhAylw\"],\"type\":\"file\",\"size\":1091536,\"hash\":\"dfe0aa7992f95f2d4731ddeb65319780\",\"mtime\":1479518055,\"user_metadata\":{},\"user_metadata_ts\":1479518055.829717,\"ranges\":[{\"range\":[0,1091535],\"chunks\":[{\"id\":[1479518055.304166,\"AhAylw\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19269\",\"/stripe/19269.144.1372878600.8.2\"],\"content/275016/678/c99/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSQgC-YCEDKXAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA1QBA\"]},{\"id\":[1479518070.897756,\"F0_2jQ\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19455\",\"/stripe/19455.283.1375200265.8.2\"],\"content/275016/ea5/3c3/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUN_FwXT_aNAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA6gBA\"]}],\"dcids\":[[1479518070.883097,\"F0_2mw\"]]}]},\"queue\":{},\"ctime\":1479518067.042391}" % (rev, count)

    else:
        # purge
        if delay: time.sleep(random.random()*2)
        return "{\"_rev\":\"%s\",\"count\":%d,\"root\":\"275016\",\"vn_hash\":\"kCzTwvEwqQpoPRFFwlK4MJqkuW8\",\"aversion\":{\"id\":[1479518055.304166,\"AhAylw\"],\"type\":\"file\",\"size\":1091536,\"hash\":\"dfe0aa7992f95f2d4731ddeb65319780\",\"mtime\":1479518055,\"user_metadata\":{},\"user_metadata_ts\":1479518055.829717,\"ranges\":[{\"range\":[0,1091535],\"chunks\":[{\"id\":[1479518055.304166,\"AhAylw\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19269\",\"/stripe/19269.144.1372878600.8.2\"],\"content/275016/678/c99/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSQgC-YCEDKXAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA1QBA\"]},{\"id\":[1479518070.897756,\"F0_2jQ\"],\"status\":\"active\",\"sn\":[\"nsos\",[\"19455\",\"/stripe/19455.283.1375200265.8.2\"],\"content/275016/ea5/3c3/2.kCzTwvEwqQpoPRFFwlK4MJqkuW8FQZ0kIAvmAhAylwVBnSUN_FwXT_aNAAAAAAAQp9Df4Kp5kvlfLUcx3etlMZeA6gBA\"]}],\"dcids\":[[1479518070.883097,\"F0_2mw\"]]}]},\"queue\":{\"purge\":[[1429837250.906498]]},\"ctime\":1479518067.042391}" % (rev, count)

        if rev == None:
            return "{\"count\":%d,\"root\":\"275016\",\"vn_hash\":\"kCzTwvEwqQpoPRFFwlK4MJqkuW8\",\"aversion\":\"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890\",\"ctime\":1479518067.042391}" % count
        else:
            return "{\"_rev\":\"%s\",\"count\":%d,\"root\":\"275016\",\"vn_hash\":\"kCzTwvEwqQpoPRFFwlK4MJqkuW8\",\"aversion\":\"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890\",\"ctime\":1479518067.042391}" % (rev, count)

dbsign = None
lock = threading.Lock()

def load_test_init(ip, args, **params):
    global dbsign 
    dbsign = subprocess.Popen(["/a/bin/dbsign", "--server"], stdout = subprocess.PIPE, stdin = subprocess.PIPE)
    v = dbsign.stdout.readline()
    assert(v == '- OK 1\n')

    state = CouchUtils.db_info(ip, args['upload'], **params)
    state['suffix'] = ''.join(chr(i) for i in state["shard_suffix"])
    state['shards'] = {}
    state['debug'] = args['upload']['debug']
    for shard in params['shards']:
        rc, stat = CouchUtils.data_shard_info(ip, shard, state, **params)
        if rc:
            state['shards'][shard] = stat
    return state

def load_test_do_work(ip, doc, args, **params):
    delay = args['upload']['delay']
    debug = args['upload']['debug']

    local = getattr(params['local'], 'local')
    if not local['init']:
        local['init'] = True
        local['conn'] = None
        local['status'] = {}
        local['counter'] = 0 
        local['heartbit'] = int(random.random() * args['upload']['heartbit'])

    if local['conn'] == None:
        local['conn'] = httplib.HTTPConnection(ip, 5050)

    local['counter'] += 1

    for i in range(params['nrevs']):
        count = i+1
        if debug: print >>sys.stderr, "%d [%s] GET /%s/%s" % (time.time(), ip, params['dbname'],doc)

        try:
            if args['upload'].has_key('dbsign') and args['upload']['dbsign']:
                lock.acquire()
                dbsign.stdin.write("%d signurl db_readonly GET - /%s/%s\n" % (local['counter'], params['dbname'], doc))
                s = dbsign.stdout.readline()
                lock.release()

                hdnea = s.split()
                assert(int(hdnea[0]) == local['counter'])
                assert(hdnea[1] == 'OK')

                local['conn'].request("GET", "/%s/%s" %(params['dbname'],doc), headers = {"EdgeAuth" : hdnea[2]})
            else:
                local['conn'].request("GET", "/%s/%s" %(params['dbname'],doc))

            resp = local['conn'].getresponse()

            out = ''
            chunk = resp.read()
            while chunk:
                out += chunk
                chunk = resp.read()

            if debug: print >>sys.stderr, "%d [%s] %s" % (time.time(), ip, out)
            obj = json.loads(out)

            if obj.has_key("_rev"):
                rev = obj['_rev'] 
            else:
                rev= None
            
            if debug: print >>sys.stderr, "%d [%s] PUT /%s/%s?_rev=%s" % (time.time(), ip, params['dbname'],doc,rev)

            payload = _get_payload(count,delay,rev)
            if args['upload'].has_key('dbsign') and args['upload']['dbsign']:
                hasher = hashlib.sha256()
                hasher.update(payload)
                payload_hash = hasher.hexdigest()

                lock.acquire()
                dbsign.stdin.write("%d signurl db_readwrite PUT %s /%s/%s\n" % (local['counter'], payload_hash, params['dbname'], doc))
                s = dbsign.stdout.readline()
                lock.release()

                hdnea = s.split()
                assert(int(hdnea[0]) == local['counter'])
                assert(hdnea[1] == 'OK')

                local['conn'].request("PUT", "/%s/%s" %(params['dbname'],doc), payload, {"EdgeAuth" : hdnea[2]})
            else:
                local['conn'].request("PUT", "/%s/%s" %(params['dbname'],doc),  payload)


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

    if local['counter'] % args['upload']['heartbit'] == local['heartbit']:
        print >>sys.stderr, "%d [%s] HEARTBIT (%d) %d %s" % (time.time(), ip, local['tid'], local['counter'] * params['nrevs'], str(sorted(local['status'].items())).replace(' ', ''))
        local['status'] = {}
    

def load_test_do_work_2(ip, doc, args, **params):
    if args['upload'].has_key('delay'): delay = args['upload']['delay']
    else: delay = False

    nrevs = params['nrevs']

    for i in range(nrevs):
        count = i+1
        out, _err = CouchAPI.read_doc_front_door(ip, doc, args['upload'], **params)
        obj=json.loads(out)
        if obj.has_key("_rev"):
            rev = obj['_rev'] 
        else:
            rev= None

        cmd =["/a/bin/couchcurl", "-s", "--dbsign-role", "server_admin",
                    "-X", "PUT", "%s:5050/%s/%s" % 
                    (ip, params['dbname'],doc), "-d", _get_payload(count,delay,rev)]

        out, _err  = CouchUtils.exec_cmd(ip, cmd,  args['upload'], **params)            

def _access_doc_front_door_404(ip, **params):
    cmd = ["/a/bin/couchcurl", "-s",
        '%s:5050/%s/doc%%2Fnot_foung%%2Fmissing'%
        (ip,params['dbname'])]
    if params['debug']: print >>sys.stderr, "%d [%s] %s" % (time.time(), ip, cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True, shell=False)
    proc.wait()
    out, _err = proc.communicate()
    if params['debug']: print >>sys.stderr, "%d [%s] %s" % (time.time(), ip, out)

def _touch_view(ip, shard, view, args, **params):
    cmd = ["/a/bin/couchcurl", "-s", 
        '%s:5986/shards%%2F%s%%2F%s%s/_design/%s/_view/dirview?limit=1&reduce=false'%
        (ip,shard,params['dbname'],args['suffix'],view)]
    if args['debug']: print >>sys.stderr, "%d [%s] %s %s" % (time.time(), ip, shard, cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True, shell=False)
    proc.wait()
    out, _err = proc.communicate()
    if args['debug']: print >>sys.stderr, "%d [%s] %s %s" % (time.time(), ip, shard, out)

def _run_open_rev(shard, docs, leadip, targetips, args, state, **params):
    start_time = time.time() * 1000

    q = Queue()
    for doc in docs:
        q.put(doc)

    stopped = Event()
    def loop(thread, targetips, counter):
        index = 0
        while not stopped.isSet():
            try:
                doc = q.get()
                if not len(doc): 
                    continue
                doc =  re.sub('/', '%2F', doc)
                ip = targetips[index % len(targetips)]
                index +=1
                cmd = ["/a/bin/couchcurl", "-s", 
                        '%s:5050/%s/%s?open_revs=all' % (ip,params['dbname'],doc)]
                if args['debug']: print >>sys.stderr, "%d [%s] %s %s" % (time.time(), ip, j, cmd)
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True, shell=False)
                out, _err = proc.communicate()
                rc = proc.wait()
                if args['debug']: print >>sys.stderr, "%d [%s] %s %d %s" % (time.time(), ip, thread, rc, out)
                adjusted_time = int(int(time.time())/params['interval'])*params['interval']
                counter[adjusted_time]+=1
                obj=json.loads(out)
            finally:
                q.task_done()

    nworkers = 3
    if args.has_key('nworkers'): nworkers = args['nworkers']

    threads = []
    for j in range(nworkers):
        counter = Counter()
        t = Thread(name="_run_open_rev_%d"%j, target=loop, args=(j,targetips, counter))
        t.daemon = True
        t.start()
        threads.append((t,counter))

    q.join()    
    print 'open_rev_elapsed_time', shard, leadip, int(time.time() * 1000 - start_time)
    stopped.set()
    for j in range(nworkers):
        q.put('')

    counter = Counter()
    # join all threads
    for t, c in threads:
        t.join()
        counter += c

    for adjusted_time in sorted(counter):
        print 'open_rev_hit',leadip,shard,adjusted_time,counter[adjusted_time]

    #line = "[open_rev_time] %s %d" % (shard, time.time() * 1000 - start_time)
    #state['out'].append(line)

def _exec_purge(shard, leadip, targetips, startkey, endkey, args, state, **params):
    random.shuffle(targetips)    
    view = urllib.quote_plus(params['viewmap'][shard])

    ip = targetips[0] 

    cmd = ["/a/bin/couchcurl", "-s", 
        '%s:5986/shards%%2F%s%%2F%s%s/_design/%s/_view/dirview?limit=%d&reduce=false&include_docs=true&startkey=%s&endkey=%s'%
        (ip,shard,params['dbname'],args['suffix'],view,args['batchsize'],startkey, endkey)]
    if args['debug']: print >>sys.stderr, "%d [%s] %s %s" % (time.time()*1000, ip, shard, cmd)

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True, shell=False)
    out, _err = proc.communicate()
    if args['debug']: print >>sys.stderr, "%d [%s] %s %s" % (time.time()*1000, ip, shard, out)
    obj=json.loads(out)

    if not obj.has_key('rows') or len(obj['rows']) == 0: 
        print >> sys.stderr, "%d [_exec_purge] %s %s %d" % (time.time(), shard, ip, state['counter'])
        return

    """
    Purge Doc
    {
        "doc_id" : ["rev_id"]
    }
    """
    purgeDoc = {}
    for doc in obj['rows']:
        key = doc['key']
        _docid = doc['doc']['_id']
        _rev = doc['doc']['_rev']
        if purgeDoc.has_key(_docid): purgeDoc[_docid].append(_rev)
        else:  purgeDoc[_docid] = [_rev]
        #purgeDoc[doc['doc']['_id']]
        
    if args['with_open_rev']:
        _run_open_rev(shard, purgeDoc.keys(), leadip, targetips, args, state, **params)
    else:    
        for ip in targetips[1:]:
            cmd = ["/a/bin/couchcurl", "-s", 
                '%s:5986/shards%%2F%s%%2F%s%s/_design/%s/_view/dirview?limit=%d&reduce=false&include_docs=true&startkey=%s&endkey=%s'%
                (ip,shard,params['dbname'],args['suffix'],view,args['batchsize'],startkey, endkey)]

            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True, shell=False)
            out, _err = proc.communicate()
            obj=json.loads(out)
        pass    

    for ip in targetips:    
        #print >>sys.stderr, "%d [%s] %s" % (time.time()*1000, ip, shard)
        cmd = ["/a/bin/couchcurl", "-s", "--dbsign-role", "server_admin",
            '%s:5986/shards%%2F%s%%2F%s%s/_purge'%(ip,shard,params['dbname'],args['suffix']), 
            '-d', '%s'%json.dumps(purgeDoc), '-H', 'Content-Type: application/json', '-X', 'POST']
        if args['debug']: print >>sys.stderr, "%d [%s] %s %s" % (time.time()*1000, ip, shard, cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True, shell=False)
        out, _err = proc.communicate()
        obj=json.loads(out)
        state['counter'] += len(obj['purged'])
        if args['debug']: print >>sys.stderr, "%d [%s] %s %s" % (time.time()*1000, ip, shard, out)
        _touch_view(ip, shard, view, args, **params)
        #print >> sys.stderr, "%d [%s] %s purge request len=%d, responce len=%d" % (time.time()*1000, ip, shard, len(purgeDoc), len(obj['purged']))
        adjusted_time = int(int(time.time())/params['interval'])*params['interval']
        print 'purge_hit',ip,shard,adjusted_time,len(purgeDoc),len(obj['purged'])

    #line =  "[purge_doc] %d %s %s" % (time.time(), shard, len(purgeDoc))
    #state['out'] = line


    key[1] = str(int(key[1])+1)    
    startkey = urllib.quote_plus(json.dumps(key))
    _exec_purge(shard, leadip, targetips, startkey, endkey, args, state, **params)     

def _run_purge(ip, task, args, **params):
    threads = []
    stats = {}
    startkey = '%5B%22' + args['root'] + '%22%5D'
    endkey = '%5B%22' + args['root'] + 'z%22%5D'
    #print >>sys.stderr, "%d [_run_purge] start" % time.time()
    for shard, nodes in args["by_range"].items():
        def start(shard,targetips,state):
            starttime = time.time() * 1000
            _exec_purge(shard, ip, targetips, startkey, endkey, args, state, **params)
            endtime = time.time() * 1000
            #line = "[purge_time] %s %d" % (shard, endtime - starttime)
            #state['out'].append(line)
            print 'purge_elapsed_time', shard, ip, int(endtime - starttime)


        targetips = []
        for node in nodes:
            targetips.append('198' + node.split('@10')[1])
        leadip = targetips[0]
        #print >>sys.stderr, "LEAD IP %s %s" % (shard, leadip)
        if ip != leadip:
            continue

        state = {
          'out' : [],
          'counter' :0
        }
        stats[shard] = state
        time.sleep(.2)
        t = Thread(name="purge_%s"%shard, target=start, args=(shard,targetips,state))   
        t.daemon = True
        t.start()
        threads.append(t)

    # join all threads
    for t in threads:
        t.join()

    #print >>sys.stderr, "%d [_run_purge] end" % time.time()
    for k, state in stats.items():
        for line in state['out']:
            print >>sys.stderr, line

            
def _all_docs(ip, args, **params):
    threads = []
    stats = {}
    startkey = '%22' + args['root'] + '%22'
    endkey = '%22' + args['root'] + 'z%22'
    limit = 1000
    if args.has_key('limit'):
        limit = args['limit']

    print >>sys.stderr, "%d [_all_docs] start" % time.time()
    for shard, nodes in args["by_range"].items():
        targetips = []
        for node in nodes:
            targetips.append('198' + node.split('@10')[1])

        leadip = targetips[0]
        if ip != leadip:
            continue

        view = urllib.quote_plus(params['viewmap'][shard])

        def start(shard, targetips, view, state):
            for ip in targetips:    
                _touch_view(ip, shard, view, args, **params)
                cmd = ["/a/bin/couchcurl", "-s", 
                        '%s:5986/shards%%2F%s%%2F%s%s/_all_docs?&startkey=%s&endkey=%s&limit=%d'%
                        (ip,shard,params['dbname'],args['suffix'],startkey,endkey,limit)]
                if args['debug']: print >>sys.stderr, "[%d] %s %s" % (time.time()*1000, shard, cmd)
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True, shell=False)
                out, _err = proc.communicate()
                if args['debug']: print >>sys.stderr, "[%d] %s %s" % (time.time()*1000, shard, out)
                obj=json.loads(out)

                if obj.has_key('rows') and len(obj['rows']):
                    line = "[all_docs] %s %s %d" % (ip, shard, len(obj['rows']))
                    state.append(line)
            pass
        pass    

        state = []
        stats[shard] = state
        time.sleep(.2)
        t = Thread(name="all_docs_%s"%shard, target=start, args=(shard,targetips, view, state))   
        t.daemon = True
        t.start()
        threads.append(t)

    # join all threads
    for t in threads:
        t.join()

    print >>sys.stderr, "%d [_all_docs] end" % time.time()
    print >>sys.stderr, "----------- All Docs --------------"
    print >>sys.stderr, "IP SHARD NDOCS"
    for k, lines in stats.items():
        for line in lines:
            print >>sys.stderr, line

def _do_dirview(ip, state, args, **params):
    elapsed_time, (out, error) = Profile.run_3(CouchAPI.read_view_front_door, ip, None, args, **params)
    bin = Profile.get_bin(elapsed_time*1000)
    state['dirview_hist'][bin] += 1

    # http status code in bin 11 & 12
    status_code = out.split('\n').pop()
    if '200' == status_code:
        state['dirview_hist'][11] += 1
    else:
        print >>sys.stderr, '_do_dirview http status code = ',status_code
        state['dirview_hist'][12] += 1
    pass    
    adjusted_time = int(int(time.time())/params['interval'])*params['interval']
    print 'dirview_hit',ip,adjusted_time

def _change_feed(ip, args, **params):
    shards = CouchUtils.shards_by_fsize(args, **params)
    CouchAPI.change_feed(ip, shards[0], args, **params)

def load_test_do_exec_task(ip, task, args, **params):
    if task == 'purge': _run_purge(ip, task, args, **params)
    if task == 'all_docs': _all_docs(ip, args, **params)
    if task == 'change_feed': _change_feed(ip, args, **params)

def load_test_done(ip, state, args, **params):
    for shard in state['shards']:
        init_stat = state['shards'][shard]
        rc, final_stat = CouchUtils.data_shard_info(ip, shard, state, **params)
        assert(rc)
        info = CouchReporting.data_shard_info_to_str(shard, init_stat, final_stat)
        print "shard %s %s" % (ip, info)
    sys.stderr.flush()        
        

