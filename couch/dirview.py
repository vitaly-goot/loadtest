#/a/bin/python2.7

def load_test_do_work(ip, doc, args, **params):
    for i in range(params['nrevs']):
        out, _err = CouchAPI.read_view_front_door(ip, doc, args['dirview'], **params)

