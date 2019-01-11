#/a/bin/python2.7
import ssl
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

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

cluster = None


def load_test_init(ip, args, **params):
    debug = args['upload']['debug']
    my_ssl_options_with_client_auth = {
        'ca_certs' : '/a/etc/ssl_ca/canonical_ca_roots.pem',
        'keyfile' : '/a/apache_cassandra/conf/apache_cassandra.ssl.all_block.private_key',
        'certfile' : '/a/apache_cassandra/conf/apache_cassandra.ssl.combined.certificate'
        }

    global cluster
    cluster = Cluster(
                contact_points = [ip],
                port = 11900,
                ssl_options = my_ssl_options_with_client_auth)


    if not cluster.metadata.keyspaces.has_key('cobra'):
        time.sleep(random.random()*3)
        if debug: print >>sys.stderr, "%d [%s] Creating keyspace cobra" % (time.time(), ip)

        session = cluster.connect()
        session.execute("""
                CREATE KEYSPACE IF NOT EXISTS cobra 
                WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'DC1800':5}
                """)

        session.execute("""
                CREATE TABLE IF NOT EXISTS cobra.testdb (
                    id text PRIMARY KEY,
                    component_path text,
                    data text
                )
                """)

        session.execute("""
                CREATE CUSTOM INDEX IF NOT EXISTS fn_prefix ON cobra.testdb (component_path)
                USING 'org.apache.cassandra.index.sasi.SASIIndex'
                """)

        session.shutdown()
        time.sleep(random.random()*2)
    pass


def load_test_do_work(ip, doc, args, **params):
    global cluster
    debug = args['upload']['debug']

    local = getattr(params['local'], 'local')
    if not local['init']:
        local['init'] = True
        local['conn'] = None
        local['status'] = {}
        local['counter'] = 0 
        local['heartbit'] = int(random.random() * args['upload']['heartbit'])

    if local['conn'] == None:
        local['conn'] = cluster.connect()
        local['conn'].set_keyspace("cobra")

    local['counter'] += 1

    #prepared = local['conn'].prepare("""
    #    INSERT INTO testdb (id, data)
    #    VALUES (?, ?)
    #    """)

    for i in range(params['nrevs']):
        count = i+1
        if debug: print >>sys.stderr, "%d [%s] GET /%s/%s" % (time.time(), ip, params['dbname'],doc)

        try:
            payload = _get_payload(count,False,None)
            query = SimpleStatement("""
                    INSERT INTO testdb (id, component_path, data)
                       VALUES (%(k)s, %(p)s, %(v)s)
                    """, consistency_level=ConsistencyLevel.ONE)
            local['conn'].execute(query, dict(k=doc, p=doc, v=payload))
            #local['conn'].execute(prepared, ('%s'%doc, '%s'%payload))

            if debug: print >>sys.stderr, "%d [%s] dbname=%s doc=%s rev=%s" % (time.time(), ip, params['dbname'],doc,rev)


        except:
            print >>sys.stderr, "%d [%s] Exception in %s" % (time.time(), ip, doc)
            traceback.print_exc(file=sys.stderr)
            local['conn'].shutdown()
            time.sleep(random.random())
            local['conn'] = None
            if local['status'].has_key(500):
                local['status'][500] += 1
            else:
                local['status'][500] = 1
            return
    pass

    if local['counter'] % args['upload']['heartbit'] == local['heartbit']:
        print >>sys.stderr, "%d [%s] HEARTBIT (%d) %d %s" % (time.time(), ip, local['tid'], local['counter'] * params['nrevs'], str(sorted(local['status'].items())).replace(' ', ''))
        local['status'] = {}
    

def load_test_done(ip, state, args, **params):
    global cluster
    cluster.shutdown()
        

