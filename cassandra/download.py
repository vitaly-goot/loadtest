#/a/bin/python2.7
import ssl
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

cluster = None

def load_test_init(ip, args, **params):
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


def load_test_do_work(ip, doc, args, **params):
    global cluster
    debug = args['download']['debug']

    local = getattr(params['local'], 'local')
    if not local['init']:
        local['init'] = True
        local['conn'] = None
        local['status'] = {}
        local['counter'] = 0 
        local['heartbit'] = int(random.random() * args['download']['heartbit'])

    if local['conn'] == None:
        local['conn'] = cluster.connect()
        local['conn'].set_keyspace("cobra")

    local['counter'] += 1

    for i in range(params['nrevs']):
        count = i+1
        if debug: print >>sys.stderr, "%d [%s] GET %d /%s/%s" % (time.time(), ip, port, params['dbname'],doc)

        try:
            query = SimpleStatement("SELECT data FROM cobra.testdb WHERE id='%s'" % doc, 
                        consistency_level=ConsistencyLevel.QUORUM)
            result = local['conn'].execute(query)
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

    if local['counter'] % args['download']['heartbit'] == local['heartbit']:
        print >>sys.stderr, "%d [%s] HEARTBIT (%d) %d %s" % (time.time(), ip, local['tid'], local['counter'] * params['nrevs'], str(sorted(local['status'].items())).replace(' ', ''))
        local['status'] = {}
        
def load_test_done(ip, state, args, **params):
    global cluster
    cluster.shutdown()
        
