#/a/bin/python2.7
from socket import error as SocketError

def load_test_do_work(ip, doc, args, **params):
    debug = args['download']['debug']

    local = getattr(params['local'], 'local')
    if not local['init']:
        local['init'] = True
        local['conn'] = {}
        local['status'] = {}
        local['counter'] = 0 
        local['heartbit'] = int(random.random() * args['download']['heartbit'])

    ports = [5050, 11971, 11972, 11973]
    for port in ports:
        #local['conn'].set_debuglevel(1)
        if not local['conn'].has_key(port) or local['conn'][port] == None:
            local['conn'][port] = httplib.HTTPConnection(ip, port)
    
    local['counter'] += 1
    port = ports[local['counter'] % len(ports)]
    conn = local['conn'][port]

    for i in range(params['nrevs']):
        count = i+1
        if debug: print >>sys.stderr, "%d [%s] GET %d /%s/%s" % (time.time(), ip, port, params['dbname'],doc)
        try:
            conn.request("GET", "/%s/%s" %(params['dbname'],doc))
            resp = conn.getresponse()
            if local['status'].has_key(resp.status):
                local['status'][resp.status] += 1
            else:   
                local['status'][resp.status] = 1

            chunk = resp.read()
            out = ''
            while chunk:
                out += chunk
                chunk = resp.read()

            if debug: print >>sys.stderr, "%d [%s] %s" % (time.time(), ip, out)
        except httplib.HTTPException as e:
            print >>sys.stderr, "%d [%s] HTTPException %s in %s" % (time.time(), ip, e, doc)
            #traceback.print_exc(file=sys.stderr)
            conn.close()
            time.sleep(random.random())
            local['conn'][port] = None
            if local['status'].has_key(500):
                local['status'][500] += 1
            else:
                local['status'][500] = 1
            return
        except SocketError as e:
            print >>sys.stderr, "%d [%s] SocketError %s in %s" % (time.time(), ip, e, doc)
            #traceback.print_exc(file=sys.stderr)
            conn.close()
            time.sleep(random.random())
            local['conn'][port] = None
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

    if local['counter'] % args['download']['heartbit'] == local['heartbit']:
        print >>sys.stderr, "%d [%s] HEARTBIT (%d) %d %s" % (time.time(), ip, local['tid'], local['counter'] * params['nrevs'], str(sorted(local['status'].items())).replace(' ', ''))
        local['status'] = {}
        
def load_test_do_work_2(ip, doc, args, **params):
    for i in range(params['nrevs']):
        out, _err = CouchAPI.read_doc_front_door(ip, doc, args['download'], **params)

