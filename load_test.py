#!/a/bin/python2.7

import socket
import time
import binascii
import sys
import traceback
import getopt
import re
import random
import os
import signal

from threading import Thread,Event,current_thread,local
from Queue import Queue
from cStringIO import StringIO

import httplib

def patch_http_response_read(func):
    def inner(*args):
        try:
            return func(*args)
        except httplib.IncompleteRead, e:
            return e.partial

    return inner

httplib.HTTPResponse.read = patch_http_response_read(httplib.HTTPResponse.read)


viewmap = {
    '00000000-07ffffff':'active_EBIMHKFDK',
    '08000000-0fffffff':'active_EFCJBBCOE',
    '10000000-17ffffff':'active_@DCDILHIK',
    '18000000-1fffffff':'active_@@ICCEMBE',
    '20000000-27ffffff':'active_CBHFBMHNK',
    '28000000-2fffffff':'active_CFBAHDMEE',
    '30000000-37ffffff':'active_FDBOCJFCK',
    '38000000-3fffffff':'active_F@HHICCHE',
    '40000000-47ffffff':'active_GFKAGF@JA',
    '48000000-4fffffff':'active_GBAFMOEAO',
    '50000000-57ffffff':'active_B@AHFANGA',
    '58000000-5fffffff':'active_BDKOLHKLO',
    '60000000-67ffffff':'active_AFJJM@N@A',
    '68000000-6fffffff':'active_AB@MGIKKO',
    '70000000-77ffffff':'active_D@@CLG@MA',
    '78000000-7fffffff':'active_DDJDFNEFO',
    '80000000-87ffffff':'active_DBABBADOD',
    '88000000-8fffffff':'active_DFKEHHADJ',
    '90000000-97ffffff':'active_ADKKCFJBD',
    '98000000-9fffffff':'active_A@ALIOOIJ',
    'a0000000-a7ffffff':'active_BB@IHGJED',
    'a8000000-afffffff':'active_BFJNBNONJ',
    'b0000000-b7ffffff':'active_GDJ@I@DHD',
    'b8000000-bfffffff':'active_G@@GCIACJ',
    'c0000000-c7ffffff':'active_FFCNMLBAN',
    'c8000000-cfffffff':'active_FBIIGEGJ@',
    'd0000000-d7ffffff':'active_C@IGLKLLN',
    'd8000000-dfffffff':'active_CDC@FBIG@',
    'e0000000-e7ffffff':'active_@FBEGJLKN',
    'e8000000-efffffff':'active_@BHBMCI@@',
    'f0000000-f7ffffff':'active_E@HLFMBFN',
    'f8000000-ffffffff':'active_EDBKLDGM@'
}

shardmap = {
    '00000000-07ffffff':0,
    '08000000-0fffffff':1,
    '10000000-17ffffff':2,
    '18000000-1fffffff':3,
    '20000000-27ffffff':4,
    '28000000-2fffffff':5,
    '30000000-37ffffff':6,
    '38000000-3fffffff':7,
    '40000000-47ffffff':8,
    '48000000-4fffffff':9,
    '50000000-57ffffff':10,
    '58000000-5fffffff':11,
    '60000000-67ffffff':12,
    '68000000-6fffffff':13,
    '70000000-77ffffff':14,
    '78000000-7fffffff':15,
    '80000000-87ffffff':16,
    '88000000-8fffffff':17,
    '90000000-97ffffff':18,
    '98000000-9fffffff':19,
    'a0000000-a7ffffff':20,
    'a8000000-afffffff':21,
    'b0000000-b7ffffff':22,
    'b8000000-bfffffff':23,
    'c0000000-c7ffffff':24,
    'c8000000-cfffffff':25,
    'd0000000-d7ffffff':26,
    'd8000000-dfffffff':27,
    'e0000000-e7ffffff':28,
    'e8000000-efffffff':29,
    'f0000000-f7ffffff':30,
    'f8000000-ffffffff':31
}



q = Queue()
docs=[]

params = {
    'debug' : False,
    'run' : False,
    'dbname' : None,
    'nworkers' : 1,
    'interval' : 2,
    'shards' : {},
    'folder' : 'loadtest%2F',
    'ndocs' : 100,
    'nrevs' : 1,
    'order' : 'rand',
    'dbs'   : 'dbs',
    'local': local()
}

target_shards={}

try:
    optlist, args = getopt.getopt(sys.argv[1:], 'D:s:i:w:n:f:r:o:dm', ["dbname=","shard_range=", "interval=", "workers=", "folder=", "ndocs=", "nrevs=", "order=", "debug", "run"])
    for o, a in optlist:
        if o in ('-s', '--shard_range'):
            try:
                val = shardmap[a] 
                params['shards'][a] = val
                target_shards[val]=True
            except:
                raise getopt.GetoptError("Unable to find shard associated with range %s" % a)
        elif o in ('-i', '--interval'):
            if int(a) in range(1,3601):
                    params['interval'] = int(a)
            else:
                raise getopt.GetoptError("Pick interval [%s] in range 1...300" % a)
        elif o in ('-w', '--workers'):
            if int(a) in range(1,1001):
                    params['nworkers'] = int(a)
            else:
                raise getopt.GetoptError("Pick number of worker threads [%s] in range 1...100" % a)
        elif o in ('-o', '--order'):
            params['order'] = a
        elif o in ('-D', '--dbname'):
            params['dbname'] = a
        elif o in ('-f', '--folder'):
            params['folder'] = a
        elif o in ('-n', '--ndocs'):
            params['ndocs'] = int(a)
        elif o in ('-r', '--nrevs'):
            params['nrevs'] = int(a)
        elif o in ('-d', '--debug'):
            params['debug'] = True
        elif o in ('-m', '--run'):
            params['run'] = True
        else:
            raise getopt.GetoptError("Internal option error [%s]" % o)
        pass
except getopt.GetoptError, e:
    print >>sys.stderr, "%s" % e
    print >>sys.stderr, ""
    print >>sys.stderr, "Usage: "
    print >>sys.stderr, "       %s -D|--dbname              couch db name" % sys.argv[0]
    print >>sys.stderr, "       %s -s|--shard_range         target specific shard(s), e.g. f8000000-ffffffff [default to 'all'] " % sys.argv[0]
    print >>sys.stderr, "       %s -w|--workers             specify number of threads for concurrent upload, 1...500 [default to 1]" % sys.argv[0]
    print >>sys.stderr, "       %s -i|--interval            emit throughput report adjusted by interval, 1..300 [default to 2]" % sys.argv[0]
    print >>sys.stderr, "       %s -f|--folder              couchdb uploading folder name, [default to loadtest/]" % sys.argv[0]
    print >>sys.stderr, "       %s -n|--ndocs               number of documents to upload, [default to 100]" % sys.argv[0]
    print >>sys.stderr, "       %s -r|--nrevs               number of revisions to upload, [default to 1]" % sys.argv[0]
    print >>sys.stderr, "       %s -n|--order               'rand' | 'desc' | 'asce' order of document upload, [default to 'rand']" % sys.argv[0]
    print >>sys.stderr, "       %s -d|--debug               enable debug messages" % sys.argv[0]
    print >>sys.stderr, "       %s -m|--run                 do run load test main body" % sys.argv[0]
    sys.exit(1)


nshards = 32
quoted_folder =  re.sub('%2F','/', params['folder'])
if len(target_shards):
    index = 0
    nshards = len(target_shards)
    while (len(docs) < params['ndocs']):
        index+=1
        qdoc = quoted_folder + str(index)
        target = (binascii.crc32(qdoc) & 0xffffffff ) / 0x8000000
        if target_shards.has_key(target):
            doc = params['folder'] +  str(index)
            docs.append(doc)
else:
    params['shards'] = shardmap
    docs = [params['folder'] + str(index) for index in range(1,params['ndocs']+1)]

params['viewmap'] = viewmap
if params['order'] == 'rand':
    random.shuffle(docs)

if params['order'] == 'desc':
    for doc in reversed(docs):
        q.put(doc)
else:
    for doc in docs:
        q.put(doc)


def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z

def myip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))  # connecting to a UDP address doesn't send packets
    return s.getsockname()[0]

ip=myip()
hits_report={}
latency_report={}

couch = CouchUtils.is_alive(ip, **params)
if couch.has_key('version') and couch['version'] == '2.1.0':
    params['dbs'] = '_dbs'

print >> sys.stderr, "%d [%s] is alive '%s'" % (time.time(), ip, str(couch))

def ctrl_c():
    return os.getppid() == 1

def ctrl_c_monitor():
    def loop():
        while True:
            if ctrl_c():
                os.kill(os.getpid(), signal.SIGKILL)
            time.sleep(3)       
        pass    
    pass    
    t = Thread(target=loop)
    t.daemon = True
    t.start()

def worker(j, args):
    setattr(params['local'], 'local', {"tid":j,'init':False})
    start_time = time.time()
    latency_report[ip,j] = Latency()
    while True:
        try:
            doc = q.get()

            elapsed_time, _res = Profile.run_4(load_test_do_work, ip, doc, args, **params)
            latency_report[ip,j].add_value(elapsed_time)
            cur_time = int(time.time())
            adjusted_time = int(cur_time - (cur_time % params['interval']))
            hits_report[ip,j,adjusted_time] += 1
        except KeyError:
            hits_report[ip,j,adjusted_time] = 1
        except:
            traceback.print_exc(file=sys.stderr)
        finally:
            hits_report[ip,j,"elapsed_time"] = time.time() - start_time
            q.task_done()

def execute(start_delay, task, args):
    def exec_task():
        time.sleep(start_delay)
        print >> sys.stderr, "%d [%s] starting execution of '%s'" % (time.time(), ip, task)
        load_test_do_exec_task(ip, task, args, **params)
    t = Thread(target=exec_task)   
    t.daemon = True
    t.start()

def monitor(interval, task, args):
    stopped = Event()
    def loop():
        while not stopped.wait(interval): # the first call is in `interval` secs
            try:
                load_test_do_monitor_task(ip, task, args, **params)
            except:
                traceback.print_exc(file=sys.stderr)
    t = Thread(target=loop)
    t.daemon = True
    t.start()    
    time.sleep(.1)
    return (stopped.set, task, args)

ctrl_c_monitor()
pid = os.getpid()

# redirect stdout to buffer
_stdout = sys.stdout
sys.stdout = StringIO()  
global_state = {}
if 'load_test_init' in globals():
    global_state = load_test_init(ip, taskConfig['workers']['args'], **params)

print >>sys.stderr, "%d [%s] Initialize: remote pid %d" % (time.time(), ip, pid)
print >> _stdout, "INIT_DONE %d [%s] remote pid '%d'" % (time.time(), ip, pid)
_stdout.flush()

# starting monitors if any
monitors = []
if 'load_test_do_exec_task' in globals():
    for exec_cfg in taskConfig['execs']:
        if  exec_cfg['ips'].has_key(ip) and exec_cfg['ips'][ip]:
            print >>sys.stderr, "%d [%s] scheduling '%s' task for execution in %d seconds" % (time.time(), ip, exec_cfg['name'], exec_cfg['start_delay'])
            execute(exec_cfg['start_delay'], exec_cfg['name'], merge_two_dicts(exec_cfg['args'], global_state))

if 'load_test_do_monitor_task' in globals():
    for mon_cfg in taskConfig['monitors']:
        if  mon_cfg['ips'].has_key(ip) and mon_cfg['ips'][ip]:
            print >>sys.stderr, "%d [%s] starting '%s' monitor" % (time.time(), ip, mon_cfg['name'])
            mon_state = {}
            if 'load_test_init_monitor' in globals():
                mon_state = load_test_init_monitor(ip, merge_two_dicts(mon_cfg['args'], global_state), **params)
                
            mon = monitor(mon_cfg['run_every'], mon_cfg['name'], merge_two_dicts(mon_cfg['args'], mon_state))
            monitors.append(mon)

while True:
    line = sys.stdin.readline()
    if line.startswith('START_LOAD'):
        break
    time.sleep(.2)    

if taskConfig.has_key('startup_delay'): 
    time.sleep(taskConfig['startup_delay'])    

if params['run']:
    print >>sys.stderr, "%d [%s] Generating %d documents in '%s' order. Concurrency level = '%d'. Folder prefix='%s'. Number of shards = %d "%(
                time.time(), ip, len(docs), params['order'], params['nworkers'], params['folder'], nshards)
                
    for j in range(params['nworkers']):
        vargs = taskConfig['workers']['args']
        t = Thread(target=worker, args=(j,vargs))
        t.daemon = True
        t.start()

    q.join()       # block until all tasks are done

else:
    print >>sys.stderr, "Node %s is not running but still collects stats" % ip

print >>sys.stderr, "%d [%s] Load done: remote pid %d" % (time.time(), ip, pid)
if taskConfig.has_key('shutdown_delay'): 
    time.sleep(taskConfig['shutdown_delay'])    

# stop monitors 
if len(monitors):
    for (stopEvent, task, args) in monitors:
        stopEvent() # raise stop event
        if 'load_test_stop_monitor' in globals():
            load_test_stop_monitor(ip, task, args, **params)
            print >>sys.stderr, "%d [%s] stop monitor: remote pid %d" % (time.time(), ip, pid)

print >> _stdout, "LOAD_DONE %s %d" % (ip, os.getpid())
_stdout.flush()

while True:
    line = sys.stdin.readline()
    if line.startswith('SHUTDOWN'):
        break
    time.sleep(.5)    


#get output
out = sys.stdout.getvalue()
# restore stdout
sys.stdout.close()
sys.stdout = _stdout  
print out.upper()

if params['debug']: print >>sys.stderr, "%d [%s] SHUTDOWN" % (time.time(), ip)
if 'load_test_done' in globals(): 
    load_test_done(ip, global_state, taskConfig['workers']['args'], **params)

for (host,thread,time),count in sorted(hits_report.items()):
    print 'hit',host,thread,time,count

for (host,thread),latency in latency_report.items():
    s = latency.to_str()
    print 'latency',host,thread,urllib.quote_plus(s)

sys.exit(0)

