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
        info = CouchReporting.data_shard_info_to_str(shard, stat, new_stat, format)
        print >>sys.stderr, "%d [%s] data_%s_mon: %s" % (int(cur_time), ip, task, info)
        shard_info[shard] = new_stat
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
        info = CouchReporting.view_shard_info_to_str(shard, stat, new_stat, format)
        print >>sys.stderr, "%d [%s] view_%s_mon: %s" % (int(cur_time), ip, task, info)
        shard_info[shard] = new_stat
    pass
    args['view_shard_info'] = shard_info

def _monitor_shards(ip, task, args, **params):
    if args['data']: _monitor_data_shards(ip, task, args, **params)
    if args['view']: _monitor_view_shards(ip, task, args, **params)

def _monitor_active_tasks(ip, task, args, **params):
    out, stderr = CouchAPI.active_tasks(ip, args, **params)
    out = out.rstrip()
    if out != "[]":
        cur_time = time.time()
        print >>sys.stderr, "%d [%s] %s_mon: %s" % (int(cur_time), ip, task, out)

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
        nshards = 1
    nshards = min(len(shards), nshards)
    for i in range(nshards):
        shard = shards[i]
        CouchCompaction.compact_data(shard, args, **params)
        CouchCompaction.compact_view(shard, args, **params)

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
        'data_shard_info' : data_shard_info, 
        'view_shard_info' : view_shard_info
    }

def load_test_do_monitor_task(ip, task, args, **params):
    if task == 'compaction': _monitor_compaction(ip, task, args, **params)
    if task == 'active_tasks': _monitor_active_tasks(ip, task, args, **params)
    if task == 'shards': _monitor_shards(ip, task, args, **params)
    if task == 'pidstat': _monitor_pidstat(ip, task, args, **params)

def load_test_init(ip, args, **params):
    db_info = CouchUtils.db_info(ip, args['admin'], **params)
    db_info['suffix'] = ''.join(chr(i) for i in db_info["shard_suffix"])
    db_info['debug'] = args['admin']['debug']
    return db_info

def load_test_do_work(ip, doc, args, **params):
    time.sleep(1)

