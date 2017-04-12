
import os, sys, subprocess
from xml.dom import minidom
import re
import ast
from threading import Thread, Lock
import Queue
import time

class AtomicCounter():
    def __init__(self, cnt, max_delay):
        self.lock = Lock()
        self.max_delay = max_delay
        self.timeout = time.time() + max_delay
        self.counter=cnt
        self.init_cnt=cnt
        self.wait = True

    def test(self):
        if debug: print >>sys.stderr, "waiting for %d machine(s) to be started... " % self.counter

        if self.wait == False:
            return False

        if time.time() > self.timeout:
            print >>sys.stderr, "maximum time out reached, did not recieve ack from %d machine(s), continue anyway" % (self.init_cnt - self.counter) 

            self.wait = False
            return False

        with self.lock: 
            self.wait = self.counter > 0
         
        return self.wait 

    def decrement(self):
        with self.lock: 
            if not self.timeout: self.timeout = time.time() + self.max_delay
            self.counter-=1

def run_remote(debug, shell, start_sync, finish_sync, testmain, config, username, ip, mode, postproc, args):
    try:
        if debug: args.append("'--debug'")
        if mode == 'run': args.append("'--run'")

        f = open("lib/profile.py", "r")
        script = f.read()
        f.close()

        f = open("lib/latency.py", "r")
        script += f.read()
        f.close()

        f = open("lib/utils.py", "r")
        script += f.read()
        f.close()


        f = open(testmain, "r")
        script += f.read()
        f.close()

        try:
            f = open(config, "r")
            script += 'taskConfig = '
            script += f.read()
            f.close()
        except:
            pass
            
        f = open("load_test.py", "r")
        script += f.read()
        f.close()

        cmd = [shell, "%s@%s" % (username, ip), "/usr/local/akamai/bin/python2.7", "<(head -c %s)" % (len(script)), ] + args
        if debug: print >>sys.stderr, cmd

        f = open("remote_script", 'w')
        f.seek(0)
        f.write(script)
        f.truncate()
        f.close()

        sp = subprocess.Popen(cmd, stdin = subprocess.PIPE, stdout=subprocess.PIPE,
                              close_fds = True, shell = False)
        sp.stdin.write(script)
        sp.stdin.flush()

        # load test initialized on target machine
        while True:
            line = sp.stdout.readline()
            if line.startswith('INIT_DONE'):
                if debug: print >>sys.stderr, line
                start_sync.decrement()
                break

        # waiting for other machines to init as well
        while start_sync.test():
            time.sleep(1)

        # starting load test  
        sp.stdin.write("START_LOAD\n")
        sp.stdin.flush()

        # load test done on target machine
        while True:
            line = sp.stdout.readline()
            if line.startswith('LOAD_DONE'):
                if debug: print >>sys.stderr, line
                finish_sync.decrement()
                break

        # waiting for other machines to finish load test 
        while finish_sync.test():
            time.sleep(1)

        # cleanup, callect statistics etc...
        sp.stdin.write("SHUTDOWN\n")
        sp.stdin.flush()


        # get stdout from all machine
        for line in sp.stdout:
            postproc.stdin.write(line)

        sp.stdin.close()
        sp.stdout.close()
        sp.wait()

        if (sp.returncode == 0): sys.exit(0)
        if (sp.returncode >> 8): sys.exit(sp.returncode >> 8)
        sys.exit((sp.returncode & 0xff) | 0x80)
    except KeyboardInterrupt:
        print "^C: Terminate remote"
        sp.terminate()
        sys.exit(1)
    pass

def quote_sh(el, ip):
    name  = el.attributes['name'].value
    val = el.attributes['value'].value
    if name == "folder":
        arg = "--%s=%s%s%%2F"%(name, val, ip)
    else:
        arg = "--%s=%s"%(name, val)
    if arg == None: return "''"
    if re.match("^[-./:0-9a-zA-Z]+$", arg): return arg
    arg = re.sub("('+)", "'\"\\1\"'", arg)
    arg = "'%s'" % arg
    arg = re.sub("^''", "", arg)
    arg = re.sub("''$", "", arg)
    return arg
        

# main
start_time = time.time()

run_tests = []
if len(sys.argv) > 1:
    run_tests = sys.argv[1:]

xmldoc = minidom.parse('config.xml')
for load_test in xmldoc.getElementsByTagName('load_test'):
    
    threads = []
    try:
        debug = int(load_test.attributes['debug'].value)
    except KeyError:
        debug = 0
    pass

    cmd = []
    try:
        cmd.append(load_test.attributes['postproc'].value)
    except KeyError:
        cmd = ["cat"]
        pass
    pass

    testname = load_test.attributes['name'].value
    if len(run_tests) and testname not in run_tests:
        continue

    config = load_test.attributes['config'].value
    f = open(config, "r")
    data = f.read()
    compiled_config = ast.literal_eval(data)
    cluster = compiled_config['workers']['ips']
    f.close()

    try:
        testmain = load_test.attributes['main'].value
    except KeyError:
        testmain = testname
    pass

    print >> sys.stderr, "----------------------------------"
    print >> sys.stderr, "running test [%s], postproc %s" % (testmain, cmd)

    postproc = subprocess.Popen(cmd, stdin=subprocess.PIPE, shell = False)
    username = load_test.getElementsByTagName('user')[0].attributes['name'].value
    shell = load_test.getElementsByTagName('shell')[0].attributes['command'].value
    i = 0
    
    # we running protocol other ssh stdin/stdout to sync up load over all target machine
    # counting number of machine started/stopped execution of the main body of load test. 
    # Synced up start/stop yields more accurate results. For example, it is necessary to stop load test on all target machines before gathering needed statistics

    # wait for all machine to start unles delay reached 
    START_DELAY = 120
    start_sync = AtomicCounter(len(cluster), START_DELAY)
    # wait for all machine to stop unless delay reached  
    FIN_DELAY = 1200  
    finish_sync = AtomicCounter(len(cluster), FIN_DELAY)

    for ip in cluster:
        i+=1
        if i%10 == 0: time.sleep(1)
        mode = cluster[ip]

        pass
        t = Thread(target=run_remote, args=(
            debug, 
            shell, 
            start_sync,
            finish_sync,
            testmain,
            config,
            username,
            ip,
            mode,
            postproc,
            [quote_sh(el, ip) for el in load_test.getElementsByTagName('arg')]))
        t.daemon = True
        t.start()
        threads.append(t)

    # join all threads
    for t in threads:
        t.join()

    postproc.stdin.close()
    postproc.wait()

    print "----------------------------------"
    print "All set in %d seconds. See you soon!" % int(time.time() - start_time)

sys.exit(0)

