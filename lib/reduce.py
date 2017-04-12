#!/usr/bin/python

import urllib
import sys
import socket
import traceback
import latency as llib
from collections import Counter

open_rev_hits=Counter()
dir_view_hits=Counter()
open_rev_elapsed_time=Counter()
purge_hits=Counter()
purge_elapsed_time=Counter()
hits={}
ip_shards = {}
shards = {}
latency = {}
cluster_latency = llib.Latency()
ops = {
   'commit':0,
   'insert':0,
   'update':0,
   'delete':0,
   'purge':0,
   'disk_size':0,
   'data_size':0
}

rest = []

def _reduce(args):
    try:
        if args[0] == 'shard':
            args.pop(0)
            ip = args.pop(0)
            shard = args.pop(0)
            ip_shards[(ip, shard)] = " ".join(args)
            if shards.has_key(shard):
                _args = shards[shard]
                _args = [int(x) + int(y) for x, y in zip(args, _args)]
                #if _args[0]: _args[6] = _args[5]/_args[0]
                shards[shard] = _args
            else:
                shards[shard] = args

            ops['commit'] += int(args[0])    
            ops['insert'] += int(args[1])
            ops['update'] += int(args[2])    
            ops['delete'] += int(args[3])
            ops['purge']  += int(args[4]) 
            ops['disk_size']   += int(args[5])   
            ops['data_size']   += int(args[6])   

        elif args[0] == 'hit':    
            if hits.has_key(args[3]):
                hits[args[3]]+=float(args[4])
            else:    
                hits[args[3]]=float(args[4])
        elif args[0] == 'latency':    
            ip = args[1]
            l = llib.Latency()
            l.from_str(urllib.unquote(args[3]))
            if latency.has_key(ip):
                latency[ip].combine(l)
            else:    
                latency[ip] = l
        elif args[0] == 'PURGE_HIT':
            purge_hits[args[3]]+=int(args[5])
            purge_hits['_'+args[2]]+=int(args[5])
            purge_hits['______overall_____']+=int(args[5])
        elif args[0] == 'PURGE_ELAPSED_TIME':
            purge_elapsed_time[args[1]]+=int(args[3])
            purge_elapsed_time['_____overall_____']+=int(args[3])
        elif args[0] == 'OPEN_REV_HIT':
            open_rev_hits[args[3]]+=int(args[4])
            open_rev_hits['_'+args[2]]+=int(args[4])
            open_rev_hits['______overall_____']+=int(args[4])
        elif args[0] == 'OPEN_REV_ELAPSED_TIME':
            open_rev_elapsed_time[args[1]]+=int(args[3])
            open_rev_elapsed_time['_____overall_____']+=int(args[3])
        elif args[0] == 'DIRVIEW_HIT':
            dir_view_hits[args[2]]+=1
            dir_view_hits['______overall_____']+=1
        else:
            rest.append(" ".join(args))
    except Exception,e:
        traceback.print_exc()


for line in sys.stdin:
    args = line.rstrip().split() 
    if len(args): _reduce(args)

"""
REPORTS
"""
print "----------- IP SHARDS ----------------"
print "IP SHARD COMMIT INSERT UPDATE DELETE PURGE DISK_SIZE DATA_SIZE"
for el in sorted(ip_shards.items(), key=lambda item: (socket.inet_aton(item[0][0]), item[0][1])):
    (ip, shard) = el[0]
    print "%s %s %s" %(ip, shard, el[1])

print "----------- SHARDS ----------------"
print "SHARD COMMIT INSERT UPDATE DELETE PURGE DISK_SIZE DATA_SIZE"
for el in sorted(shards.items(), key=lambda item: item[0]):
    map(str, el[1])
    print "%s %s" %(el[0], " ".join(map(str, el[1])))

print "----------- IP LATENCY (ms) ----------------"
for (ip, l) in latency.items(): 
    cluster_latency.combine(l)    
    print ip,l.report()
print 'cluster',cluster_latency.report(distribution=True)    


print "----------- OPS ----------------"
if ops['insert']:
    overhead = (ops['disk_size'] - ops['data_size'])/ ops['insert']
    doc_size = ops['data_size'] / ops['insert']
else:
    overhead = ops['disk_size'] - ops['data_size']
    doc_size = ops['data_size']
print "cluster {\'commit\':%d,\'insert\':%d,\'update\':%d,\'delete\':%d,\'purge\':%d,\'disk_size\':%d,\'overhead\':%d,\'data_size\':%d,\'doc_size\':%d}" % (
            ops['commit'], ops['insert'], 
            ops['update'], ops['delete'], ops['purge'], 
            ops['disk_size'], overhead, 
            ops['data_size'], doc_size 
            )

print "----------- TIME HITS ----------------"
for el in sorted(hits.items()):
    (timestamp, hits) = el
    if timestamp != 'elapsed_time': hits = int(hits)
    print '%s %s'%(timestamp,hits)

print "----------- PURGE HITS ----------------"
for t in sorted(purge_hits):
    print t,purge_hits[t]

print "----------- PURGE ELAPSED TIME ----------------"
for title in sorted(purge_elapsed_time):
    print "%s %.3f" % (title, float(purge_elapsed_time[title])/1000)

print "----------- OPEN REV HITS ----------------"
for t in sorted(open_rev_hits):
    print t,open_rev_hits[t]

print "----------- OPEN REV ELAPSED TIME ----------------"
for title in sorted(open_rev_elapsed_time):
    print "%s %.3f" % (title, float(open_rev_elapsed_time[title])/1000)

print "----------- DIR VIEW HITS ----------------"
for t in sorted(dir_view_hits):
    print t,dir_view_hits[t]

print "----------- REST ----------------"
for line in sorted(rest):
    print line
 
sys.exit(0)
