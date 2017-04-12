#/a/bin/python2.7

import os

config = '''
[default]
url:/ g2o:super=password sign-headers:^X-Akamai-ACS-Action$
'''

def load_test_init(ip, args, **params):
    with open(os.path.expanduser("~/.g2ocurl"),'w+') as myfile:
        myfile.write(config)

    state = CouchUtils.db_info(ip, args['mkdir'], **params)
    state['suffix'] = ''.join(chr(i) for i in state["shard_suffix"])
    state['shards'] = {}
    for shard in params['shards']:
        rc, stat = CouchUtils.data_shard_info(ip, shard, args['mkdir'], **params)
        if rc:
            state['shards'][shard] = stat
    return state

def load_test_do_work(ip, doc, args, **params):
    for i in range(params['nrevs']):
        out, _err = CobraAPI.mkdir(ip, doc, args['mkdir'], **params)

def load_test_done(ip, state, args, **params):
    for shard in state['shards']:
        init_stat = state['shards'][shard]
        rc, final_stat = CouchUtils.data_shard_info(ip, shard, args['mkdir'], **params)
        assert(rc)
        info = CouchReporting.data_shard_info_to_str(shard, init_stat, final_stat)
        print "shard %s %s" % (ip, info)
    sys.stderr.flush()        
