#/a/bin/python2.7

def load_test_do_work(ip, doc, args, **params):
    if args['dirview'].has_key('debug'): debug = args['dirview']['debug']
    else: debug = False

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

    options = CouchUtils.parse_args(args['dirview'])
    key = "%s" %  doc.replace("%2F", "/%2F").split('%2F')
    startkey = urllib.quote_plus(key.replace('\'', '"').replace(' ', ''))
    options.append('startkey=%s' % startkey)

    if args.has_key('viewname'): viewname = args['viewname']
    else: viewname = 'dirview'

    time.sleep(random.random()*4)

    for i in range(params['nrevs']):
        if debug: print >>sys.stderr, "%d [%s] GET /%s/_design/active/_view/%s?%s" % (time.time(), ip, params['dbname'],viewname,"&".join(options))

        try: 
            local['conn'].request("GET", "/%s/_design/active/_view/%s?%s" %(params['dbname'],viewname,"&".join(options)))
            resp = local['conn'].getresponse()
            if local['status'].has_key(resp.status):
                local['status'][resp.status] += 1
            else:   
                local['status'][resp.status] = 1
        except httplib.BadStatusLine:
            print >>sys.stderr, "%d [%s] BadStatusLine in GET /%s/_design/active/_view/%s?%s" % (time.time(), ip, params['dbname'],viewname,"&".join(options))
            #traceback.print_exc(file=sys.stderr)
            local['conn'].close()
            time.sleep(random.random())
            local['conn'] = None
            if local['status'].has_key(500):
                local['status'][500] += 1
            else:
                local['status'][500] = 1
            break
        except SocketError as e:
            print >>sys.stderr, "%d [%s] SocketError %s in GET /%s/_design/active/_view/%s?%s" % (time.time(), ip, e, params['dbname'],viewname,"&".join(options))
            #traceback.print_exc(file=sys.stderr)
            local['conn'].close()
            time.sleep(random.random())
            local['conn'] = None
            if local['status'].has_key(600):
                local['status'][600] += 1
            else:
                local['status'][600] = 1
            break
        pass    

        out = ''
        chunk = resp.read()
        while chunk:
            out += chunk
            chunk = resp.read()

        if debug: print >>sys.stderr, "%d [%s] %s" % (time.time(), ip, out)
    pass

    if local['counter'] % args['dirview']['heartbit'] == local['heartbit']:
        print >>sys.stderr, "%d [%s] HEARTBIT (%d) %d %s" % (time.time(), ip, local['tid'], local['counter'] * params['nrevs'], str(sorted(local['status'].items())).replace(' ', ''))
        local['status'] = {}


def _load_test_do_work(ip, doc, args, **params):
    for i in range(params['nrevs']):
        out, _err = CouchAPI.read_view_front_door(ip, doc, args['dirview'], **params)

