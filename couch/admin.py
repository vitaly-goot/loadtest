import os

def _find_compacted_files(dbname, suffix):
    proc1 = subprocess.Popen(["find", "/ghostcache/edgedata/", "-name", "*compact*"], stdout=subprocess.PIPE, close_fds=True, shell=False)
    proc2 = subprocess.Popen(["grep", "%s%s"%(dbname,suffix)], stdout=subprocess.PIPE, stdin=proc1.stdout, close_fds=True, shell=False)
    out, _err = proc2.communicate()
    return out.rstrip().split("\n")

    proc1 = subprocess.Popen(["du", "-a", "/ghostcache/edgedata/data/shards/"], stdout=subprocess.PIPE, close_fds=True, shell=False)
    proc2 = subprocess.Popen(["grep", "%s%s.couch"%(dbname,suffix)], stdout=subprocess.PIPE, stdin=proc1.stdout, close_fds=True, shell=False)
    proc3 = subprocess.Popen(["sort",  "-rn"], stdout=subprocess.PIPE, stdin=proc2.stdout, close_fds=True, shell=False)
    proc4 = subprocess.Popen(["head", "-1"], stdout=subprocess.PIPE, stdin=proc3.stdout, close_fds=True, shell=False)
    proc5 = subprocess.Popen(["awk", "-F/", "{print $6}"], stdout=subprocess.PIPE, stdin=proc4.stdout, close_fds=True, shell=False)
    out, _err = proc5.communicate()

def _monitor_dbstat(ip, task, args, **params):
    cur_time = time.time()
    cmd = ["cat", "/a/etc/nsds_db/tables/stat.csv"]
    out, err  = CouchUtils.exec_cmd(ip, cmd, args, **params)            
    if err:
        print >>sys.stderr, "%d [%s] dbstat_mon: ERROR %s" % (int(cur_time), ip, err) 
        return

    lines = out.rsplit('\n')
    for i in range(5,len(lines)):
        row = lines[i].split(",")
        if len(row) == 12 and row[5] == 'process':
            print >>sys.stderr, "%d [%s] dbstat_mon: %s %s %s %s" % (int(cur_time), ip, row[3], row[4], row[6], row[7])

def _monitor_node_state(ip, task, args, **params):
    cur_time = time.time()
    cmd = ["/a/sbin/sabersqlcli", "select node,ipa,is_online from nsds_dba_node_state where is_online!=1 order by node"]
    out, err  = CouchUtils.exec_cmd(ip, cmd, args, **params)            
    if err:
        print >>sys.stderr, "%d [%s] node_state_mon: ERROR %s" % (int(cur_time), ip, err) 
        return

    lines = out.rsplit('\n')
    annotations = lines[2].split()[1:]
    metrics = lines[3].split()
    info = dict(zip(annotations,metrics))
    if args.has_key('format'):
        format = args['format']
    else:
        format = 'csv'

    if format != 'csv':
        info_out = str(info)
    else:    
        info_out = ' '.join(metrics)

    print >>sys.stderr, "%d [%s] pidstat_mon: %s" % (int(cur_time), ip, info_out)
        

def _monitor_pidstat(ip, task, args, **params):
    cur_time = time.time()
    cmd = ["pidstat", "-hrudvI", "-C", "beam", "5", "1"]
    out, err  = CouchUtils.exec_cmd(ip, cmd, args, **params)            
    if err:
        print >>sys.stderr, "%d [%s] pidstat_mon: ERROR %s" % (int(cur_time), ip, err) 
        return

    lines = out.rsplit('\n')
    annotations = lines[2].split()[1:]
    metrics = lines[3].split()
    info = dict(zip(annotations,metrics))
    if args.has_key('format'):
        format = args['format']
    else:
        format = 'csv'

    if format != 'csv':
        info_out = str(info)
    else:    
        info_out = ' '.join(metrics)

    print >>sys.stderr, "%d [%s] pidstat_mon: %s" % (int(cur_time), ip, info_out)
        

def _monitor_data_shards(ip, task, args, **params):
    cur_time = time.time()
    shard_info = {}
    for shard, stat in args['data_shard_info'].items():
        rc, new_stat = CouchUtils.data_shard_info(ip, shard, args, **params)
        assert(rc)

        if args.has_key('format'):
            format = args['format']
        else:    
            format = 'csv'
        pass
        try:
            info = CouchReporting.data_shard_info_to_str(shard, stat, new_stat, format)
            print >>sys.stderr, "%d [%s] data_%s_mon: %s" % (int(cur_time), ip, task, info)
            shard_info[shard] = new_stat
        except:    
            print >>sys.stderr, "%d [%s] data_%s_mon: ERROR %s" % (int(cur_time), ip, task, shard)
        pass    
    pass
    args['data_shard_info'] = shard_info

def _monitor_view_shards(ip, task, args, **params):
    cur_time = time.time()
    shard_info = {}
    for shard, stat in args['view_shard_info'].items():
        rc, new_stat = CouchUtils.view_shard_info(ip, shard, args, **params)
        assert(rc)

        if args.has_key('format'):
            format = args['format']
        else:    
            format = 'csv'
        try:    
            info = CouchReporting.view_shard_info_to_str(shard, stat, new_stat, format)
            print >>sys.stderr, "%d [%s] view_%s_mon: %s" % (int(cur_time), ip, task, info)
            shard_info[shard] = new_stat
        except:    
            print >>sys.stderr, "%d [%s] view_%s_mon: ERROR %s" % (int(cur_time), ip, task, shard)
        pass    
    pass
    args['view_shard_info'] = shard_info

def _monitor_shards(ip, task, args, **params):
    if args['data']: _monitor_data_shards(ip, task, args, **params)
    if args['view']: _monitor_view_shards(ip, task, args, **params)


def _monitor_active_tasks(ip, task, args, **params):
    cur_time = time.time()
    out, stderr = CouchAPI.active_tasks(ip, args, **params)
    out = out.rstrip()
    obj = json.loads(out)
    if out != "[]":
        print >>sys.stderr, "%d [%s] %s_mon: %s" % (int(cur_time), ip, task, out)

    for e in obj:
        if e.has_key('type'):
            if e['type'] == 'indexer':
                #key = (e['type'],e['category'],e['database'])
                key = (e['type'],e['database'])
            elif e['type'] == 'view_compaction':
                key = (e['type'],'v',e['database'])
            elif e['type'] == 'database_compaction': # couch2 
                key = (e['type'],'d',e['database'])
            elif e['type'] == 'Database Compaction': # couch1    
                key = (e['type'],e['task'])
            elif e['type'] == 'replication':
                key = e['replication_id']
            elif e['type'] == 'View Group Indexer': # couch1
                key = (e['type'],e['task'])
            elif e['type'] == 'View Group Compaction': # couch1
                key = (e['type'],e['task'])
            elif e['type'] == 'Replication': # couch1
                key = e['task']
            else:
                key = 'zhopa'
                continue
            pass    

            if args['active_tasks'].has_key(key):
                prev_e = args['active_tasks'][key]
                prev_time = prev_e['time']
                elapsed_time = cur_time - prev_time
                if e['type'] == 'replication':
                    changes_done = e['docs_written'] - prev_e['docs_written'] 
                    cmd = ["/a/bin/couchcurl", "-s", "%s" % e['target']]
                    out, _err  = CouchUtils.exec_cmd(ip, cmd, args, **params)            
                    current_stat = json.loads(out)
                    if current_stat.has_key('doc_count'):
                        e['target_data_size'] = current_stat['data_size']
                        e['target_disk_size'] = current_stat['disk_size']
                elif e['type'] == 'View Group Compaction':
                    m = re.search(r'.* ([\d]+) of ([\d]+) .* (\([\d]+%\))', e['status'])
                    changes_done = int(m.group(1)) - prev_e['changes_done']
                    e['changes_done'] =  int(m.group(1)) 
                    e['progress'] = m.group(3)
                elif e['type'] == 'View Group Indexer':
                    changes_done = int(e['status'].split()[1])- prev_e['changes_done'] 
                    e['changes_done'] = int(e['status'].split()[1])
                    e['progress'] = e['status'].split()[5]
                elif e['type'] == 'Replication':
                    e['changes_done'] = int(e['status'].split()[4].strip('#'))
                    changes_done = e['changes_done'] - prev_e['changes_done'] 
                    target_shard = e['task'].split()[3]
                    cmd = ["/a/bin/couchcurl", "-s", "%s" % target_shard]
                    out, _err  = CouchUtils.exec_cmd(ip, cmd, args, **params)            
                    current_stat = json.loads(out)
                    if current_stat.has_key('doc_count'):
                        e['target_data_size'] = current_stat['other']['data_size']
                        e['target_disk_size'] = current_stat['disk_size']
                elif  e['type'] == 'Database Compaction': # couch1 data compaction
                    changes_done = int(e['status'].split()[1]) - prev_e['changes_done'] 
                    e['changes_done'] = int(e['status'].split()[1])
                    e['progress'] = e['status'].split()[5]
                else: # indexer & view_compaction & database_compaction
                    changes_done = e['changes_done'] - prev_e['changes_done'] 
                pass
                if elapsed_time:
                    thrpt_sec = changes_done / elapsed_time
                else:
                    thrpt_sec = changes_done
                e['count'] = prev_e['count'] + 1
                if e['type'] == 'indexer':
                    f_view = ""
                    if args.has_key('gc_root'):
                        gc_root = args['gc_root']
                    else:
                        gc_root = 'edgedata'
                    dirname = "/ghostcache/" + gc_root + "/view/." + e['database'] + "_design/mrview/"
                    for file in os.listdir(dirname):
                        if re.match(r'^[\w\d]*.view$', file):
                            f_view = dirname + file
                            break
                    rc, stat = CouchUtils.stat_file(f_view)
                    if rc:
                        f_view_size = stat.st_size/1024/1024
                    else:
                        f_view_size = 0
                    #info = "%s %s (%d%%) %d %d %d %d %d" % (e['database'], e['category'], e['progress'], thrpt_sec, e['changes_done'], e['seq'], e['count'], f_view_size)
                    info = "%s (%d%%) %d %d %d %d" % (e['database'], e['progress'], thrpt_sec, e['changes_done'], e['count'], f_view_size)
                elif e['type'] == 'view_compaction':
                    f_compact = ""
                    if args.has_key('gc_root'):
                        gc_root = args['gc_root']
                    else:
                        gc_root = 'edgedata'
                    dirname = "/ghostcache/" + gc_root + "/view/." + e['database'] + "_design/mrview/"
                    for file in os.listdir(dirname):
                        if re.match(r'^[\w\d]*.compact.view$', file):
                            f_compact = dirname + file
                            break
                    rc, stat = CouchUtils.stat_file(f_compact)
                    if rc:
                        f_compact_size = stat.st_size/1024/1024
                    else:
                        f_compact_size = 0

                    info = "%s (%d%%) %d %d %d" % (e['database'], e['progress'], thrpt_sec, e['count'], f_compact_size)
                elif e['type'] == 'database_compaction': 
                    if args.has_key('gc_root'):
                        gc_root = args['gc_root']
                    else:
                        gc_root = 'edgedata'
                    f_couch = "/ghostcache/" + gc_root + "/data/" + e['database'] + ".couch"
                    rc, stat = CouchUtils.stat_file(f_couch)
                    if rc:
                        f_couch_size = stat.st_size/1024/1024
                    else:
                        f_couch_size = 0

                    f_meta = f_couch + ".compact.meta"
                    rc, stat = CouchUtils.stat_file(f_meta)
                    if rc:
                        f_meta_size = stat.st_size/1024/1024
                    else:
                        f_meta_size = 0

                    f_data = f_couch + ".compact.data"
                    rc, stat = CouchUtils.stat_file(f_data)
                    if rc:
                        f_data_size = stat.st_size/1024/1024
                    else:
                        f_data_size = 0
                    info = "%s (%d%%) %d %d %d %d %d" % (e['database'], e['progress'], thrpt_sec, e['count'], f_couch_size, f_meta_size, f_data_size)
                elif e['type'] == 'Database Compaction': # couch1 data compaction
                    f_couch = "/ghostcache/edgedata/data/" + e['task'] + ".couch"
                    rc, stat = CouchUtils.stat_file(f_couch)
                    if rc:
                        f_couch_size = stat.st_size/1024/1024
                    else:
                        f_couch_size = 0

                    f_compact = "/ghostcache/edgedata/data/" + e['task'] + ".couch.compact"
                    rc, stat = CouchUtils.stat_file(f_compact)
                    if rc:
                        f_compact_size = stat.st_size/1024/1024
                    else:
                        f_compact_size = 0
                    info = "%s %s %d %d %d %d" % (e['task'], e['progress'], thrpt_sec, e['count'], f_couch_size, f_compact_size)
                elif e['type'] == 'replication':
                    info = "%s %d %d %d %d %d" % (e['source'], e['progress'], thrpt_sec, e['target_disk_size'], e['target_data_size'], e['count'])
                elif e['type'] == 'View Group Compaction':
                    f_compact = ""
                    dirname = "/ghostcache/edgedata/view/." + "/".join(e['task'].split('/')[0:3]) + "_design/"
                    for file in os.listdir(dirname):
                        if re.match(r'^[\w\d]*.compact.view$', file):
                            f_compact = dirname + file
                            break
                    rc, stat = CouchUtils.stat_file(f_compact)
                    if rc:
                        f_compact_size = stat.st_size/1024/1024
                    else:
                        f_compact_size = 0

                    info = "%s %s %d %d %d" % (e['task'], e['progress'], thrpt_sec, e['count'], f_compact_size)
                elif e['type'] == 'View Group Indexer':
                    f_view = ""
                    dirname = "/ghostcache/edgedata/view/." + e['task'].split()[0] + "_design/"
                    for file in os.listdir(dirname):
                        if re.match(r'^[\w\d]*.view$', file):
                            f_view = dirname + file
                            break
                    rc, stat = CouchUtils.stat_file(f_view)
                    if rc:
                        f_view_size = stat.st_size/1024/1024
                    else:
                        f_view_size = 0
                    info = "%s %s %d %d %d" % (e['task'], e['progress'], thrpt_sec, e['count'], f_view_size)
                elif e['type'] == 'Replication':
                    info = "%s %d %d %d %d" % (e['task'], thrpt_sec, e['target_disk_size'], e['target_data_size'], e['count'])
                else:
                    info = "---"
                pass    
                print >>sys.stderr, "%d [%s] %s_mon: %s" % (int(cur_time), ip, e['type'], info)
            else:
                e['count'] = 0
                if e['type'] == 'View Group Compaction' or e['type'] == 'View Group Indexer' or e['type'] == 'Database Compaction':
                    e['changes_done'] = 0
                    e['progress'] = '(0%)'
                elif e['type'] == 'Replication' or e['type'] == 'replication':
                    e['changes_done'] = 0
                    e['target_disk_size'] = 0
                    e['target_data_size'] = 0
                pass    
            pass    
            e['time'] = cur_time
            args['active_tasks'][key] = e
        pass 
    pass

def _monitor_compaction(ip, task, args, **params):
    cur_time = time.time()
    files = _find_compacted_files(params['dbname'], args['suffix'])
    if args.has_key('format'):
        format = args['format']
    else:    
        format = 'csv'

    for filename in files:

        rc, stat = CouchUtils.stat_file(filename)
        if not rc:
            continue

        cur_size = stat.st_size

        if not args['active_compaction'].has_key(filename):
            if filename.endswith('.couch.compact'):
                shard = filename.rsplit('/')[5]
                rc, info = CouchUtils.data_shard_info(ip, shard, args, **params)
                assert(rc)
                data_size = info['other']['data_size']
                disk_size = info['disk_size']
                doc_count = info['doc_count']
                doc_size = float(data_size)/doc_count
                if format == 'csv':
                    print >>sys.stderr, "%d [%s] %s_mon: %s %s %.3f" % (int(cur_time), ip, task, filename, info['db_name'], doc_size)
                else:    
                    print >>sys.stderr, "%d [%s] %s_mon: %s {\'db_name\': %s, \'doc_size\': %.3f}" % (int(cur_time), ip, task, filename, info['db_name'], doc_size)

            elif filename.endswith('.compact.view'):
                shard = filename.rsplit('/')[5]
                rc, info = CouchUtils.view_shard_info(ip, shard, args, **params)
                assert(rc)
                data_size = info['view_index']['data_size']
                disk_size = info['view_index']['disk_size']
                if format == 'csv':
                    print >>sys.stderr, "%d [%s] %s_mon: %s %s" % (int(cur_time), ip, task, filename, info['name'])
                else:
                    print >>sys.stderr, "%d [%s] %s_mon: %s {\'view_name\': %s}" % (int(cur_time), ip, task, filename, info['name'])
            else:
                continue


            args['active_compaction'][filename] = {
                'start_size': cur_size,
                'start_time': cur_time,
                'last_size': cur_size, 
                'last_time': cur_time,
                'data_size': data_size,
                'last_fin_estimation': 0,
                'latency' : Latency(frame=60, resolution=1)
                }

            continue        
        pass        
            
        start_time = args['active_compaction'][filename]['start_time']
        start_size = args['active_compaction'][filename]['start_size']
        last_time = args['active_compaction'][filename]['last_time']
        last_size = args['active_compaction'][filename]['last_size']
        data_size = args['active_compaction'][filename]['data_size']
        last_fin_estimation = args['active_compaction'][filename]['last_fin_estimation']

        #print >>sys.stderr, cur_size, args['active_compaction'][filename]

        progress_so_far = 100.0 - float((data_size-cur_size))/data_size*100
        wMBs_last_cycle = float(cur_size - last_size) / (cur_time - last_time) / 1024 / 1024
        wMBs_so_far = float(cur_size - start_size) / (cur_time - start_time) / 1024 / 1024
        if wMBs_last_cycle:
            fin_estimation = (data_size-cur_size) / 1024 / 1024 / wMBs_last_cycle
            args['active_compaction'][filename]['latency'].add_value(cur_time+fin_estimation)
            prediction_report = args['active_compaction'][filename]['latency'].report()
        else:
            fin_estimation = 0
            prediction_report = "stalled"

        if format == 'csv':
            print >>sys.stderr, "%d [%s] %s_mon: %s %.3f %.3f %.3f %d %s" % (int(cur_time), ip, task, filename, wMBs_last_cycle, wMBs_so_far, progress_so_far, last_fin_estimation - fin_estimation, prediction_report)
        else:
            print >>sys.stderr, "%d [%s] %s_mon: %s {'speed': %.3f, 'speed_so_far': %.3f, 'progress_so_far': %.3f, 'miss': %d, 'prediction':%s" % (int(cur_time), ip, task, filename, wMBs_last_cycle, wMBs_so_far, progress_so_far, last_fin_estimation - fin_estimation, prediction_report)

        args['active_compaction'][filename]['last_time'] = cur_time
        args['active_compaction'][filename]['last_size'] = cur_size
        args['active_compaction'][filename]['last_fin_estimation'] = fin_estimation

    pass 

def _run_compaction(ip, task, args, **params):
    shards = CouchUtils.shards_by_fsize(args, **params)
    if args.has_key('nshards'): 
        nshards = args['nshards']
    else:    
        nshards = 2
    nshards = min(len(shards), nshards)
    for i in range(nshards):
        shard = shards[i]
        CouchCompaction.compact_data(shard, args, **params)
        #CouchCompaction.compact_view(shard, args, **params)

def load_test_do_exec_task(ip, task, args, **params):
    if task == 'compaction': _run_compaction(ip, task, args, **params)

def load_test_init_monitor(ip, args, **params):
    data_shard_info = {}
    view_shard_info = {}
    for shard in params['shards']:
        rc, info = CouchUtils.data_shard_info(ip, shard, args, **params)
        if rc:
            data_shard_info[shard] = info

        rc, info = CouchUtils.view_shard_info(ip, shard, args, **params)
        if rc:
            view_shard_info[shard] = info
    pass

    return {
        'suffix' : args['suffix'], 
        'active_compaction' : {}, 
        'active_tasks' : {}, 
        'data_shard_info' : data_shard_info, 
        'view_shard_info' : view_shard_info
    }

def load_test_do_monitor_task(ip, task, args, **params):
    if task == 'compaction': _monitor_compaction(ip, task, args, **params)
    if task == 'active_tasks': 
        _monitor_active_tasks(ip, task, args, **params)
        _monitor_dbstat(ip, task, args, **params)
    if task == 'shards': _monitor_shards(ip, task, args, **params)
    if task == 'pidstat': _monitor_pidstat(ip, task, args, **params)
    if task == 'node_state': _monitor_node_state(ip, task, args, **params)

def load_test_init(ip, args, **params):
    db_info = CouchUtils.db_info(ip, args['admin'], **params)
    db_info['suffix'] = ''.join(chr(i) for i in db_info["shard_suffix"])
    db_info['debug'] = args['admin']['debug']
    return db_info

def load_test_do_work(ip, doc, args, **params):
    time.sleep(1)

