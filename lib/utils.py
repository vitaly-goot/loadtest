import subprocess 
import urllib
import sys
import time
import json
import pprint


class CouchUtils(object):
    @staticmethod
    def parse_args(args):
        options = []

        if args.has_key('group'): options.append('group=%s' % args['group'])

        if args.has_key('group_level'): options.append('group_level=%d' % args['group_level'])

        if args.has_key('include_docs'): options.append('include_docs=%s' % args['include_docs'])

        if args.has_key('reduce'): options.append('reduce=%s' % args['reduce'])

        if args.has_key('skip'): options.append('skip=%d' % args['skip'])

        if args.has_key('startkey'): options.append('startkey=%s' % args['startkey'])

        if args.has_key('endkey'): options.append('endkey=%s' % args['endkey'])

        if args.has_key('limit'): options.append('limit=%d' % args['limit'])
        else: options.append('limit=%d' % 1000)

        return options

    @staticmethod
    def exec_cmd(ip, cmd, args, **params):
        if args['debug']: print >>sys.stderr, "%d [%s] %s" % (time.time(), ip, cmd)
        devnull = open("/dev/null", "r+")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=devnull, close_fds=True, shell=False)
        out = ""
        for line in proc.stdout:
            out+=line
        if args['debug']: print >>sys.stderr, "%d [%s] %s" % (time.time(), ip, out)
        proc.stdout.close()
        proc.wait()
        return out, proc.returncode

    @staticmethod
    def shards_by_fsize(args, **params):
        dbname = params['dbname']
        suffix = args['suffix']
        proc1 = subprocess.Popen(["du", "-a", "/ghostcache/edgedata/data/shards/"], stdout=subprocess.PIPE, close_fds=True, shell=False)
        proc2 = subprocess.Popen(["grep", "%s%s.couch"%(dbname,suffix)], stdout=subprocess.PIPE, stdin=proc1.stdout, close_fds=True, shell=False)
        proc3 = subprocess.Popen(["sort",  "-rn"], stdout=subprocess.PIPE, stdin=proc2.stdout, close_fds=True, shell=False)
        proc4 = subprocess.Popen(["awk", "-F/", "{print $6}"], stdout=subprocess.PIPE, stdin=proc3.stdout, close_fds=True, shell=False)
        out, _err = proc4.communicate()
        return out.rstrip().split(os.linesep)

    @staticmethod
    def db_info(ip, args, **params):
        cmd = ["/a/bin/couchcurl", "-s", "%s:5986/dbs/%s"%(ip,params['dbname'])]
        out, _err  = CouchUtils.exec_cmd(ip, cmd, args, **params)            
        return json.loads(out)

    @staticmethod
    def data_shard_info(ip, shard, args, **params):
        cmd = ["/a/bin/couchcurl", "-s", 
                    "%s:5986/shards%%2F%s%%2F%s%s"%
                    (ip,shard,params['dbname'],args['suffix'])] 
        out, _err  = CouchUtils.exec_cmd(ip, cmd, args, **params)            
        obj = json.loads(out)
        if obj.has_key('doc_count'):
            return True, obj
        return False, ''

    @staticmethod
    def view_shard_info(ip, shard, args, **params):
        view = urllib.quote_plus(params['viewmap'][shard])
        cmd = ["/a/bin/couchcurl", "-s",
                '%s:5986/shards%%2F%s%%2F%s%s/_design/%s/_info'%
                (ip,shard,params['dbname'],args['suffix'],view)]
        out, _err  = CouchUtils.exec_cmd(ip, cmd, args, **params)            
        obj = json.loads(out)
        if obj.has_key('view_index'):
            return True, obj
        return False, ''

    @staticmethod
    def stat_file(fileName):
        try:
            stat = os.stat(fileName)
            return True, stat
        except:
            pass
        return False, ''    
        
class CobraAPI(object):
    @staticmethod
    def mkdir(ip, doc, args, **params):
        cmd = ["/a/bin/g2ocurl", "-s",
                "-H", "X-Akamai-ACS-Action: version=1&action=mkdir", 
                "-X", "PUT", "%s:8210/%s"%(
                    ip, doc)]
        return CouchUtils.exec_cmd(ip, cmd, args, **params)

class CouchAPI(object):        
    @staticmethod
    def active_tasks(ip, args, **params):
        cmd = ["/a/bin/couchcurl", "-s", "-X", "GET", "%s:5986/_active_tasks"%ip]
        return CouchUtils.exec_cmd(ip, cmd, {'debug':False}, **params)

    @staticmethod
    def read_doc_front_door(ip, doc, args, **params):
        cmd = ["/a/bin/couchcurl", "-s", "-X", "GET",
                    "%s:5050/%s/%s"%(
                    ip, params['dbname'],doc)]
        return CouchUtils.exec_cmd(ip, cmd, args, **params)

    @staticmethod
    def read_view_front_door(ip, doc, args, **params):
        options = CouchUtils.parse_args(args)
        if doc != None: 
            key = "%s" %  doc.replace("%2F", "/%2F").split('%2F')
            startkey = urllib.quote_plus(key.replace('\'', '"').replace(' ', ''))
            options.append('startkey=%s' % startkey)

        if args.has_key('viewname'): viewname = args['viewname']
        else: viewname = 'dirview'

        cmd = ["/a/bin/couchcurl", "-sw", "%{http_code}", 
            '%s:5050/%s/_design/active/_view/%s?%s'%
            (ip,params['dbname'],viewname,"&".join(options))]
        return CouchUtils.exec_cmd(ip, cmd, args, **params)    

    @staticmethod
    def read_view_back_door(ip, shard, args, **params):
        options = CouchUtils.parse_args(args)

        if args.has_key('viewname'): viewname = args['viewname']
        else: viewname = 'dirview'

        view = urllib.quote_plus(params['viewmap'][shard])

        cmd = ["/a/bin/couchcurl", "-s", 
                '%s:5986/shards%%2F%s%%2F%s%s/_design/%s/_view/%s?%s'%
                (ip,shard,params['dbname'],args['suffix'],view,viewname,"&".join(options))]

        return CouchUtils.exec_cmd(ip, cmd, args, **params)    

    @staticmethod
    def all_docs_back_door(ip, shard, args, **params):
        options = CouchUtils.parse_args(args)

        view = urllib.quote_plus(params['viewmap'][shard])

        cmd = ["/a/bin/couchcurl", "-s", 
                '%s:5986/shards%%2F%s%%2F%s%s/_all_docs?%s'%
                (ip,shard,params['dbname'],args['suffix'],"&".join(options))]
        return CouchUtils.exec_cmd(ip, cmd, args, **params)    

    @staticmethod
    def all_docs_front_door(ip, doc, args, **params):
        options = CouchUtils.parse_args(args)
        options.append('startkey="%s"' % doc)

        cmd = ["/a/bin/couchcurl", "-s", "-X", "GET", 
                "%s:5050/%s/_all_docs?%s"%(
                ip,params['dbname'],"&".join(options))]
        return CouchUtils.exec_cmd(ip, cmd, args, **params)

    @staticmethod
    def change_feed(ip, shard, args, **params):
        options = []
        options.append('feed=continuous')
        options.append('heartbeat=6000')
        options.append('stype=main_only')
        if args.has_key('include_docs'): options.append('include_docs=%s' % args['include_docs'])
        else: options.append('include_docs=true')
        if args.has_key('since'): options.append('since=%d' % args['since'])
        else: options.append('since=0')
        view = urllib.quote_plus(params['viewmap'][shard])
        cmd = ["/a/bin/couchcurl", "-s",
                '%s:5986/shards%%2F%s%%2F%s%s/_changes?%s'%
                (ip,shard,params['dbname'],args['suffix'],"&".join(options))]
        if args['debug']: print >>sys.stderr, "%d [%s] %s" % (time.time(), ip, cmd)
        devnull = open("/dev/null", "r+")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=devnull, close_fds=True, shell=False)
        line = 0
        if args.has_key('report_progress'):  report_progress = args['report_progress']
        else : report_progress = 0
        while True:
            proc.stdout.readline()
            line+=1
            if report_progress and 0 == line % report_progress:
                print >>sys.stderr, "%d [%s] change feed %s progress %d" % (time.time(), ip, shard, line)
            if args['debug']: print >>sys.stderr, "%d [%s] %d" % (time.time(), ip, line)
            if proc.poll() is not None: break
        proc.stdout.close()
        proc.wait()
        return proc.returncode


class CouchReporting(object):
    """ 
    v5
    state[shard] = {
    "db_name":"shards/c8000000-cfffffff/testdb.1429225170",
    "doc_count":30278,
    "doc_del_count":0,
    "update_seq":82757,
    "purge_seq":0,
    "compact_running":false,
    "disk_size":845492328,
    "other":{"data_size":20564700},
    "instance_start_time":"1430157481027285",
    "disk_format_version":5,
    "committed_update_seq":82757
    }

    v6
    state[shard] = {
        u'update_seq': 1369, 
        u'disk_size': 41590897, 
        u'purge_seq': 0, 
        u'doc_count': 226, 
        u'compact_running': False, 
        u'db_name': u'shards/38000000-3fffffff/db1802.1427331810', 
        u'doc_del_count': 0, 
        u'instance_start_time': u'1430172236342470', 
        u'committed_update_seq': 1369, 
        u'data_size': 305234, 
        u'disk_format_version': 6
    }
    """
    @staticmethod
    def data_shard_info_to_str(shard, init_stat, final_stat, format = 'csv'):
        commits =  final_stat['committed_update_seq'] - init_stat['committed_update_seq']
        disk_size =  final_stat['disk_size'] - init_stat['disk_size']
        if final_stat['disk_format_version'] == 5:
            data_size = final_stat['other']['data_size'] - init_stat['other']['data_size']
            fragmentation = float(final_stat['disk_size'] - final_stat['other']['data_size']) / final_stat['other']['data_size'] * 100
        else:
            data_size = final_stat['data_size'] - init_stat['data_size']
            fragmentation = float(final_stat['disk_size'] - final_stat['data_size']) / final_stat['data_size'] * 100
        pass    

        inserts = final_stat['doc_count'] - init_stat['doc_count']
        updates = final_stat['update_seq'] - init_stat['update_seq'] - inserts
        deletes = final_stat['doc_del_count'] - init_stat['doc_del_count']
        purges = final_stat['purge_seq'] - init_stat['purge_seq']
        if inserts:
            overhead = (disk_size - data_size) / inserts
        else:
            overhead = disk_size - data_size
        pass

        if format == 'csv':
            return "%s %d %d %d %d %d %d %d %d %d" % (
                shard,
                commits,
                inserts,
                updates,
                deletes,
                purges,
                disk_size,
                data_size,
                overhead,
                fragmentation
            )
        else:
            info = {
                'shard':shard,
                'commits':commits,
                'inserts':inserts,
                'updates':updates,
                'deletes':deletes,
                'purges':purges,
                'disk_size':disk_size,
                'data_size':data_size,
                'overhead':overhead,
                'fragmentation':"%.3f"%fragmentation
            }
            return str(info)
        pass    

    @staticmethod
    def view_shard_info_to_str(shard, init_stat, final_stat, format = 'csv'):
        updates = final_stat['view_index']['update_seq'] - init_stat['view_index']['update_seq']
        purges = final_stat['view_index']['purge_seq'] - init_stat['view_index']['purge_seq']
        disk_size = final_stat['view_index']['disk_size'] - init_stat['view_index']['disk_size']
        data_size = final_stat['view_index']['data_size'] - init_stat['view_index']['data_size']
        fragmentation = float(final_stat['view_index']['disk_size'] - final_stat['view_index']['data_size'])/final_stat['view_index']['data_size'] * 100
        waiting_clients = final_stat['view_index']['waiting_clients']
        if updates:
            overhead = (disk_size - data_size) / updates
        else:
            overhead = disk_size - data_size
        pass

        if format == 'csv':
            return "%s %d %d %d %d %d %d %d" % (
                shard,
                updates,
                purges,
                disk_size,
                data_size,
                waiting_clients,
                overhead,
                fragmentation
            )
        else:     
            info = {
                'shard':shard,
                'updates':updates,
                'purges':purges,
                'disk_size':disk_size,
                'data_size':data_size,
                'waiting_clients':waiting_clients,
                'overhead':overhead,
                'fragmentation':"%.3f"%fragmentation
            }
            return str(info)
        pass    

class CouchCompaction(object):
    @staticmethod
    def compact_data(shard, args, **params):
        dbname = params['dbname']
        suffix = args['suffix']
        cmd = ["/a/bin/couchcurl", "-X", "POST", "-H", "Content-Type: application/json", "-s", "--dbsign-role", "server_admin", 
                "127.0.0.1:5986/shards%%2F%s%%2F%s%s/_compact"%(
                shard,dbname,suffix)]
        if params['debug']: print >>sys.stderr, cmd
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True, shell=False)
        out, _err = proc.communicate()
        if params['debug']: print >>sys.stderr, out

    # /a/bin/couchcurl -X POST -H "Content-Type: application/json" -s --dbsign-role server_admin '127.0.0.1:5986/shards%2F'$SHARD'%2F'$DB'/_compact/'$VIEW''
    @staticmethod
    def compact_view(shard, args, **params):
        dbname = params['dbname']
        suffix = args['suffix']
        view = urllib.quote_plus(params['viewmap'][shard])
        cmd = ["/a/bin/couchcurl", "-X", "POST", "-H", "Content-Type: application/json", "-s", "--dbsign-role", "server_admin", 
                "127.0.0.1:5986/shards%%2F%s%%2F%s%s/_compact/%s"%(
                shard,dbname,suffix,view)]
        if params['debug']: print >>sys.stderr, cmd
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True, shell=False)
        out, _err = proc.communicate()
        if params['debug']: print >>sys.stderr, out
