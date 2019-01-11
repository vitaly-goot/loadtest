#!/usr/bin/python

# $Id: //sandbox/lisiecki/scripts/gwshplex.py#11 $

#TODO@! Figure out why remote gwshplex does not terminate reliably when the ssh connection closes.  I don't think this is new to dbsign, but it should get fixed while I am working on these changes.

import sys
import getopt
import re
import struct
import Queue
import socket
import threading
import os
import time
import select
import subprocess
import hashlib

ACK_EVERY = 16384
UNACK_BLOCK = 200000
DEBUG_PREFIX = "?"

remote_shell = "gwsh"
dbsign = False
dbsignrole = "db_readonly"

class HttpRequestInterceptor(object):
    def __init__(self, max_request, request_cb):
        self.max_request = max_request
        self.request_cb = request_cb
        self.buf = ""
        self.pos = 0
        self.state = 0
        self.headers = []
        self.hash_state = hashlib.sha256()
        self.cl = None
        self.chunked = False
        pass

    def send(self, data):
        state = self.state
        buf = self.buf + data
        pos = self.pos
        while True:
            if state == 0:
                # Read headers
                i = buf.find("\n", pos)
                if i == -1: break
                j = i
                if i > pos and buf[i-1] == "\r": j = i-1
                if j == pos:
                    # Empty line => move on to the body if we have
                    # some headers already, otherwise ignore it.
                    pos = i + 1
                    if self.headers: state = 1
                    continue
                self.headers.append(buf[pos:j])
                pos = i+1
            elif state == 1:
                # Parse headers
                state = 3
                self.request_line = self.headers[0].split(" ")
                hdrs = {}
                for h in self.headers[1:]:
                    i = h.find(":")
                    if i == -1 or i == 0:
                        print >>sys.stderr, DEBUG_PREFIX, "Bad header line: %s" % h
                        continue
                    fld = h[:i].strip()
                    fldl = fld.lower()
                    if not h:
                        print >>sys.stderr, DEBUG_PREFIX, "Bad header field %s in: %s" % (fld, h)
                        continue
                    v = h[i+1:].strip(" ")
                    if fldl in hdrs:
                        hdrs[fldl] = (hdrs[fldl][0], hdrs[fldl][1] + " " + v)
                    else:
                        hdrs[fldl] = (fld, v)
                        pass
                    pass
                self.headers = hdrs
                method = self.request_line[0].upper()
                if "content-length" in hdrs:
                    self.cl = int(hdrs["content-length"][1])
                    if self.cl < 0: raise ValueError("Negative cl")
                    pass
                if "transfer-encoding" in hdrs and "chunked" in hdrs["transfer-encoding"][1].lower():
                    self.chunked = True
                    pass
                if method == "GET" or method == "HEAD":
                    if self.chunked or self.cl is not None:
                        raise ValueError("content-length/transfer-encoding not allowed in GET/HEAD request")
                    state = 5
                    continue
                if self.chunked and self.cl is not None:
                    raise ValueError("cl and chunked cannot be used together in the request")
                if not self.chunked and self.cl is None: self.cl = 0
                if self.chunked: self.cl = 0
                pass
            elif state == 2:
                # Gobble \n or \r\n after a chunk.  Ignore any other junk
                if pos + 1 > len(buf):
                    break
                elif buf[pos] == "\n":
                    pos += 1
                    state = 3
                else:
                    pos += 1
                    continue
                pass
            elif state == 3:
                # Read body data and/or find a chunk:

                # We have a cl or are in the middle of a chunk:
                if self.cl != 0:
                    d = buf[pos:pos+self.cl]
                    # TODO@! body hash here!!
                    if not d: break
                    self.hash_state.update(d)
                    pos += len(d)
                    self.cl -= len(d)
                    if self.cl == 0 and self.chunked: state = 2
                    continue

                # Not chunked => we are done!
                if not self.chunked: state = 5; continue

                # Chunked => look for next chunk header
                i = buf.find("\n", pos)
                if i == -1: break
                j = pos
                while not buf[j].isspace(): j += 1
                self.cl = int(buf[pos:j], 16)
                if self.cl < 0: raise ValueError("Negative cl")
                pos = i+1
                if not self.cl: state = 4
                pass
            elif state == 4:
                # Trailers
                i = buf.find("\n", pos)
                if i == -1: break
                if i == pos:
                    pos += 1
                    state = 5
                    continue
                elif i == pos+1 and buf[i] == "\r":
                    pos += 2
                    state = 5
                    continue
                pos = i+1
                pass
            elif state == 5:
                # Request complete!
                self.request_cb(buf[:pos], self.request_line, self.headers, self.hash_state)

                # Remove the request from the buffer and reset the
                # state.
                state = 0
                buf = buf[pos:]
                pos = 0
                self.headers = []
                self.hash_state = hashlib.sha256()
                self.cl = None
                self.chunked = False
            pass

        if self.max_request and len(self.buf) >= self.max_request:
            raise ValueError("Request size limit exceeded")

        self.state = state
        self.buf = buf
        self.pos = pos

        return len(data)

    pass

dbsign_sp = None
dbsign_lock = None
def dbsign_request(target_socket, request, request_line, headers, hash_state):
    # Compute the signature
    method = request_line[0]
    url = request_line[1]
    # Exclude the querystring...  This is not correct, so comment it out.
    # i = url.find("?")
    # if i >= 0: url = url[:i]
    body_hash = ""
    if method.upper() not in ("GET", "HEAD"):
        body_hash = hash_state.hexdigest()
        if body_hash == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" and method.upper() not in ("PUT", "POST"):
            # Requests with empty bodies that are also not PUT/POST
            # can skip signing of the body.
            body_hash = ""
            pass
        pass
    print >>sys.stderr, DEBUG_PREFIX, "Ready to dbsign: %s %s %s" % (body_hash, method, url)
    with dbsign_lock:
        dbsign_sp.stdin.write("%s %s %s\n" % (body_hash, method, url))
        dbsign_sp.stdin.flush()
        l = dbsign_sp.stdout.readline()
        pass
    signature = ""
    if l:
        if l[-1:] == "\n": l = l[:-1]
        signature = "EdgeAuth: %s\n" % l
    else:
        print >>sys.stderr, DEBUG_PREFIX, "dbsign error: no output"
        pass
    print >>sys.stderr, DEBUG_PREFIX, "Signature is %s" % signature[:-1]

    # Modify the request to include the signature
    i = request.find("\n")
    assert(i >= 0)
    request = request[:i+1] + signature + request[i+1:]
    print >>sys.stderr, DEBUG_PREFIX, "==================== FULL REQUEST ====================\n%s\n==================== END REQUEST ====================" % request

    # Send the modified request
    while request:
        n = target_socket.send(request)
        if not n: raise IOError("Empty write?")
        request = request[n:]
        pass

    pass

class Queue2(Queue.Queue):
    def __init__(self, *args, **kwargs):
        Queue.Queue.__init__(self, *args, **kwargs)
        (self.queue_sock_wait, self.queue_sock_send) = socket.socketpair()
        self.queue_sock_wait.setblocking(0)
        self.queue_sock_send.setblocking(0)
        pass

    def close():
        self.queue_sock_wait.close()
        self.queue_sock_send.close()
        pass

    def put(self, *args, **kwargs):
        Queue.Queue.put(self, *args, **kwargs)
        self.queue_sock_send.send(".")
        pass

    def get_or_recv(self, sock, n):
        try:
            self.queue_sock_wait.recv(1024)
        except IOError:
            pass
        try:
            item = self.get(block = False)
            return ('queue', item)
        except Queue.Empty:
            pass

        p = select.poll()
        p.register(sock, select.POLLIN)
        p.register(self.queue_sock_wait, select.POLLIN)
        while True:
            print >>sys.stderr, DEBUG_PREFIX, "get_or_recv: poll"
            evs = p.poll()
            print >>sys.stderr, DEBUG_PREFIX, "get_or_recv: evs = %s" % (evs,)
            for (fd, events) in evs:
                if fd == sock.fileno():
                    print >>sys.stderr, DEBUG_PREFIX, "recving"
                    msg = sock.recv(n)
                    print >>sys.stderr, DEBUG_PREFIX, "recved %s bytes" % len(msg)
                    return ('recv', msg)
                elif fd == self.queue_sock_wait.fileno():
                    self.queue_sock_wait.recv(1024)
                    try:
                        item = self.get(block = False)
                        return ('queue', item)
                    except Queue.Empty:
                        pass
                    pass
                pass
            pass
        pass
    pass

def peer_in(peer, dispatch):
    try:
        ibuf = ""
        while True:
            tmp = os.read(peer.fileno(), 1024)
            # print >>sys.stderr, DEBUG_PREFIX, "tmp=[%s]" % tmp
            if not tmp:
                dispatch(None, None, None)
                return
            ibuf += tmp
            while len(ibuf) >= 11:
                (msg, flow, msglen) = struct.unpack("!cQH", ibuf[:11])
                if len(ibuf) < msglen+11:
                    break
                dispatch(msg, flow, ibuf[11:msglen+11])
                ibuf = ibuf[msglen+11:]
                pass
            pass
        pass
    except IOError:
        dispatch(None, None, None)
        pass
    pass

def peer_out(peer, peer_out_queue):
    # TODO: IOError should be fatal?  Or maybe peer_in noticing is good enough?
    while True:
        print >>sys.stderr, DEBUG_PREFIX, "peer_out asleep"
        (msg, flow, data) = peer_out_queue.get(block = True)
        print >>sys.stderr, DEBUG_PREFIX, "peer_out awake: %s" % ((msg, flow, len(data)),)
        msg = struct.pack("!cQH", msg, flow, len(data)) + data
        peer.write(msg)
        peer.flush()
        pass
    pass

def local_in(sock, flow, peer_out_queue, ack_queue, local_in_queue):
    print >>sys.stderr, DEBUG_PREFIX, "local_in starting for %s" % (sock,)
    try:
        unack = 0
        while True:
            (tp, msg) = local_in_queue.get_or_recv(sock, 1024)
            if (tp == 'queue'):
                print >>sys.stderr, DEBUG_PREFIX, "local_in done via peer for %s" % (sock,)
                return
            #print >>sys.stderr, DEBUG_PREFIX, "local_in recv [%s] len=%s" % (msg, len(msg))
            print >>sys.stderr, DEBUG_PREFIX, "local_in recv len=%s" % len(msg)
            if not msg:
                # Todo: tell the sender thread too?
                peer_out_queue.put(("c", flow, ""))
                print >>sys.stderr, DEBUG_PREFIX, "local_in done for %s" % (sock,)
                return
            peer_out_queue.put(("d", flow, msg))
            unack += len(msg)
            try:
                while True:
                    unack -= ack_queue.get(block = (unack >= UNACK_BLOCK))
            except Queue.Empty:
                pass
            pass
        pass
    except IOError:
        peer_out_queue.put(("c", flow, ""))
        print >>sys.stderr, DEBUG_PREFIX, "local_in done via IOError for %s" % (sock,)
        return
    pass

def local_out(sock, flow, local_out_queue, peer_out_queue):
    # TODO@!: Catch IOError and inform someone of the error?
    print >>sys.stderr, DEBUG_PREFIX, "local_out starting for %s" % (sock,)
    unack = 0
    while True:
        msg = local_out_queue.get(block = True)
        if not msg:
            print >>sys.stderr, DEBUG_PREFIX, "local_out done for %s" % (sock,)
            return
        while msg:
            n = sock.send(msg)
            msg = msg[n:]
            unack += n
            if unack >= ACK_EVERY:
                peer_out_queue.put(("a", flow, struct.pack("!I", unack)))
                unack = 0
                pass
            pass
        pass
    pass

def local_connect(addr, flow, peer_out_queue, ack_queue, local_out_queue, local_in_queue):
    try:
        sock = socket.socket()
        sock.connect(addr)
        print >>sys.stderr, DEBUG_PREFIX, "Connect to %s succeeded on sock %s" % (addr, sock)
    except IOError:
        print >>sys.stderr, DEBUG_PREFIX, "Connect to %s failed" % (addr,)
        peer_out_queue.put(("f", flow, ""))
        return
    sock_out = sock
    if dbsign:
        sock_out = HttpRequestInterceptor(100<<20, lambda *args: dbsign_request(sock, *args))
        pass
    t = threading.Thread(target = local_out,
                         args = (sock_out, flow, local_out_queue, peer_out_queue))
    t.daemon = True
    t.start()
    local_in(sock, flow, peer_out_queue, ack_queue, local_in_queue)
    t.join()
    sock.close()
    pass

def do_remote(peer0, peer1):
    global dbsign

    peer_out_queue = Queue.Queue()
    flows = {}

    if dbsign:
        if not os.path.exists("/usr/local/akamai/bin/dbsign"):
            print >>sys.stderr, DEBUG_PREFIX, "/usr/local/akamai/bin/dbsign not found.  Disabling dbsign mode"
            dbsign = False
        else:
            command = ["/usr/local/akamai/bin/dbsign"]
            if dbsignrole != "db_readonly": command += ["-r", dbsignrole]
            command += ["-m", "stdin",
                        "-b", "stdin",
                        "-u", "stdin"]
            global dbsign_sp
            global dbsign_lock
            dbsign_sp = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            dbsign_lock = threading.Lock()
            print >>sys.stderr, DEBUG_PREFIX, "Started dbsign."
            pass
        pass

    def dispatch(msg, flow, data):
        if msg is None:
            print >>sys.stderr, DEBUG_PREFIX, "dispatch: peer closed."
            os._exit(0)
            return
        print >>sys.stderr, DEBUG_PREFIX, "dispatch: msg=%s flow=%d data_len=[%s]" % (msg, flow, len(data))
        if msg == "n":
            (port,) = struct.unpack("!H", data[:2])
            addr = data[2:]
            ack_queue = Queue.Queue()
            local_out_queue = Queue.Queue()
            local_in_queue = Queue2()
            # TODO: Look for existing flow to prevent a "leak"?  We trust peer anyway, so probably not important.
            flows[flow] = {"ack": ack_queue, "local_out": local_out_queue, "local_in": local_in_queue}
            tc = threading.Thread(target = local_connect,
                                  args = ((addr, port), flow, peer_out_queue, ack_queue, local_out_queue, local_in_queue))
            print >>sys.stderr, DEBUG_PREFIX, "Starting connect thread %s for %s" % (tc, (addr, port))
            tc.daemon = True
            tc.start()
        elif msg == "d":
            if flow in flows and data:
                flows[flow]["local_out"].put(data)
                pass
            pass
        elif msg == "c":
            if flow in flows:
                flows[flow]["local_out"].put("")
                flows[flow]["local_in"].put("")
                pass
            pass
        elif msg == "a":
            if flow in flows:
                flows[flow]["ack"].put(struct.unpack("!I", data)[0])
                pass
            pass
        
        pass

    t = threading.Thread(target = peer_out,
                         args = (peer1, peer_out_queue))
    t.daemon = True
    t.start()

    t2 = threading.Thread(target = peer_in,
                          args = (peer0, dispatch))
    t2.daemon = True
    t2.start()

    ctrl_c_handler(True)

    pass

def local_accepted(sock, flow, peer_out_queue, ack_queue, local_out_queue, local_in_queue):
    t = threading.Thread(target = local_out,
                         args = (sock, flow, local_out_queue, peer_out_queue))
    t.daemon = True
    t.start()
    local_in(sock, flow, peer_out_queue, ack_queue, local_in_queue)
    t.join()
    sock.close()
    pass

def local_accept(pt, flows, flowlock, flowctr, peer_out_queue):
    sock = pt["sock"]

    while True:
        s2, address = sock.accept()
        with flowlock:
            flow = flowctr[0] = flowctr[0] + 1
            ack_queue = Queue.Queue()
            local_out_queue = Queue.Queue()
            local_in_queue = Queue2()
            flows[flow] = {"ack": ack_queue, "local_out": local_out_queue, "local_in": local_in_queue}
            peer_out_queue.put(("n", flow, struct.pack("!H", pt["dport"]) + pt["dhost"]))
            ta = threading.Thread(target = local_accepted,
                                  args = (s2, flow, peer_out_queue, ack_queue, local_out_queue, local_in_queue))
            ta.daemon = True
            ta.start()
        pass

    pass

def do_local(target, ports):
    peer_out_queue = Queue.Queue()
    flows = {}
    flowlock = threading.Lock()
    flowctr = [0]

    def dispatch(msg, flow, data):
        if msg is None:
            print >>sys.stderr, DEBUG_PREFIX, "dispatch: peer closed."
            os._exit(0)
            return
        with flowlock:
            print >>sys.stderr, DEBUG_PREFIX, "dispatch: msg=%s flow=%d data_len=[%s]" % (msg, flow, len(data))
            if msg == "d":
                if flow in flows and data:
                    flows[flow]["local_out"].put(data)
                    pass
                pass
            elif msg == "c" or msg == "f":
                if flow in flows:
                    flows[flow]["local_out"].put("")
                    flows[flow]["local_in"].put("")
                    pass
                pass
            elif msg == "a":
                if flow in flows:
                    flows[flow]["ack"].put(struct.unpack("!I", data)[0])
                    pass
                pass
            pass
        pass

    # Make sockets
    for pt in ports:
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", pt["sport"]))
        sock.listen(5)
        pt["sock"] = sock
        pass

    # Start secure connection to remote machine
    myself_path = os.path.join(sys.path[0], os.path.basename(sys.argv[0]))
    with open(myself_path, "r") as myself_file:
        myself_data = myself_file.read()
        pass

    command = ([remote_shell, "-2",
                # "-l", remote_user,
                target,
                "`ls -r /usr/local/akamai/bin/python2.? | head -n 1`", "<(head -c %s)" % len(myself_data),
                "--internal-remote"]
               # + sys.argv[1:]
               )
    if dbsign: command.extend(["--dbsign", "--dbsignrole", dbsignrole])
    print >>sys.stderr, DEBUG_PREFIX, command
    sp = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    sp.stdin.write(myself_data)
    sp.stdin.flush()

    # Start accept threads
    for pt in ports:
        ta = threading.Thread(target = local_accept,
                              args = (pt, flows, flowlock, flowctr, peer_out_queue))
        ta.daemon = True
        ta.start()
        pass

    # Run the protocol over the connection
    t = threading.Thread(target = peer_out,
                         args = (sp.stdin, peer_out_queue))
    t.daemon = True
    t.start()

    t2 = threading.Thread(target = peer_in,
                          args = (sp.stdout, dispatch))
    t2.daemon = True
    t2.start()

    ctrl_c_handler()

    pass

def ctrl_c_handler(monitor_parent = False):
    try:
        while True:
            time.sleep(monitor_parent and 1 or 120)
            if monitor_parent and os.getppid() == 1:
                # We could log, but the sshd parent is gone, so any
                # beneficial output would be lost, and maybe we would
                # end up deadlocking, so only bad things can happen if
                # we try to log.
                _os.exit(1)
                pass
            pass
    except:
        print >>sys.stderr, DEBUG_PREFIX, "Bye bye"
        os._exit(0)
        pass
    pass

def main():
    global DEBUG_PREFIX
    global remote_shell
    global dbsign
    global dbsignrole

    ports = []
    remote = False
    target = None

    try:
        optlist, args = getopt.getopt(sys.argv[1:], 'L:', ["internal-remote", "gwsh", "ssh", "dbsign", "dbsignrole="])
        for o, a in optlist:
            if o in ('-L'):
                m = re.match("^(\\d+)(?::([^:]*):(\\d*))?$", a)
                if not m: raise getopt.GetoptError("Bad -L value: " + a)
                _pf  = {"sport": int(m.group(1)),
                        "dhost": "127.0.0.1",
                        "dport": int(m.group(1))}
                if m.group(2): _pf["dhost"] = m.group(2)
                if m.group(3): _pf["dport"] = int(m.group(3))
                if [ _dummy for _dummy in ports if _dummy["sport"] == _pf["sport"] ]:
                    raise getopt.GetoptError("Source port reused: " + a)
                ports.append(_pf)
                pass
            elif o in ('--internal-remote',):
                remote = True
            elif o in ('--gwsh',):
                remote_shell = "gwsh"
            elif o in ('--ssh',):
                remote_shell = "ssh"
            elif o in ('--dbsign',):
                dbsign = True
            elif o in ('--dbsignrole',):
                dbsign = True
                if a not in ('db_readonly', 'db_readwrite', 'db_admin', 'server_admin'):
                    raise getopt.GetoptError("Bad --dbsignrole: [%s]" % a)
                dbsignrole = a
            else:
                raise getopt.GetoptError("Internal option error [%s]" % o)
            pass
        if remote:
            if len(args):
                raise getopt.GetoptError("Extra options not allowed in remote mode")
            pass
        elif len(args) != 1:
            raise getopt.GetoptError("Exactly one fixed argument is required")
        else:
            (target,) = args
            pass
        pass
    except getopt.GetoptError, e:
        print >>sys.stderr, DEBUG_PREFIX, "%s" % e
        print >>sys.stderr, DEBUG_PREFIX, ""
        print >>sys.stderr, DEBUG_PREFIX, "Usage: %s [--ssh] [-L source_port[:[dest_host]:[dest_port]]]+ [user@]host" % sys.argv[0]
        print >>sys.stderr, DEBUG_PREFIX, "  Connections use gwsh by default."
        print >>sys.stderr, DEBUG_PREFIX, " --ssh              Use ssh instead of gwsh."
        print >>sys.stderr, DEBUG_PREFIX, " -L                 Forward a port.  Repeat to forward multiple ports."
        print >>sys.stderr, DEBUG_PREFIX, "                    dest_host defaults to 127.0.0.1."
        print >>sys.stderr, DEBUG_PREFIX, "                    dest_port default to source_port."
        print >>sys.stderr, DEBUG_PREFIX, " --dbsign           use /usr/local/akamai/bin/dbsign (if present) to sign all"
        print >>sys.stderr, DEBUG_PREFIX, "                    requests."
        print >>sys.stderr, DEBUG_PREFIX, " --dbsignrole ROLE  select the db role to use with dbsign.  Default is"
        print >>sys.stderr, DEBUG_PREFIX, "                    db_readonly.  Other roles: db_readwrite db_admin"
        print >>sys.stderr, DEBUG_PREFIX, "                    server_admin.  This option implies --dbsign."
        sys.exit(1)
        pass

    if remote:
        DEBUG_PREFIX = "R"
        do_remote(sys.stdin, sys.stdout)
    else:
        DEBUG_PREFIX = "L"
        do_local(target, ports)
        pass

    pass


main()
