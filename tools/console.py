#!/usr/bin/env python

import sys
import os.path
import socket
import pickle
import pprint
import getopt


################
# A console to each CM agent
# author: an.ning@aliyun-inc 2010-1-17
# serve as both human interface or api interface.
# default connect to 127.0.0.1
################


has_json = True
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        try:
            import simplejson_purepp as json
        except ImportError:
            has_json = False

PWD = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(PWD)) #get ./../

import config
from lib.net_io import NetHead
import mod.packet as packet
is_master = False
try:
    import mod.ui_api as uiapi
    is_master = True
except ImportError:
    pass

dummy = False # if True, run in non-interacting mode

def _connect (ip, port):
    sock = socket.socket (socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt (socket.SOL_TCP, socket.TCP_SYNCNT, 1)
    except socket.error, e: 
        print >> sys.stderr, "[warnning] cannot set TCPSYNCNT to 1"
    try:
        sock.connect ((ip, int(port)))
    except socket.error, e:
        print >> sys.stderr, "cannot connect to %s:%s, %s" % (ip, str(port), str (e))
        sock.close ()
        return None
    if not dummy:
        print "connected to %s:%s" % (ip, str(port)) 
    return sock

def output_error (err_msg):
    if dummy:
        output_data ({'status': False, 'message': str(err_msg)})
    else:
        print >> sys.stderr, str(err_msg)

def process_cmd (sock, arr, is_api=False):
    """ arr is command and argument list.
        If interact with server ok returns True, otherwise returns False.
        Note that when command err, nothing will be sent, also returns False.
        leave the caller to close socket.
        """
    assert isinstance (arr, (list, tuple))
    assert len (arr) > 0
    cmd_map = {
        'status': {'cmd': 'status', 'arg_len':0},
        'status_fd': {'cmd': 'status', 'arg_len':0},
        'status_q': {'cmd': 'status', 'arg_len':0},
    }
    if is_master:
        cmd_map['push_all'] = {'cmd': 'push_all', 'arg_len':0}
        cmd_map['push'] = {'cmd':'push', 'arg_len':None}
        cmd_map['bind_all'] = {'cmd': 'bind_all', 'arg_len':0}
        cmd_map['bind'] = {'cmd': 'bind', 'arg_len':1}
        cmd_map['update_all'] = {'cmd': 'update_all', 'arg_len':0}
        cmd_map['update'] = {'cmd': 'update', 'arg_len':1}
        cmd_map['cancel_policy'] = {'cmd':'cancel_policy', 'arg_len':3}
    if arr[0] in ['help', "?"]:
        print "Available commands:"
        cmds = cmd_map.keys ()
        cmds.sort ()
        print "\n".join (map (lambda x: "\t" + x, cmds))
        return True
    if not cmd_map.has_key (arr[0]):
        output_error ("no such cmd : %s, type 'help' to list available commands" % (arr[0]))
        return False
    buf = None
    if arr[0] == 'status':
        buf = packet.StatusReq.make ().serialize ()
    elif arr[0] == 'status_fd':
        buf = packet.StatusFDReq.make ().serialize ()
    elif arr[0] == 'status_q':
        buf = packet.StatusQueueReq.make ().serialize ()
    else:
        cmd = cmd_map[arr[0]]['cmd']
        arg_len = cmd_map[arr[0]]['arg_len']
        if arg_len != None:
            args = arr[1:arg_len + 1]
        else:
            args = arr[1:]
        try:
            if arr[0] in ['push', 'bind'] and not is_api:
                # translate rolenames into roleid
                args = uiapi.cmui_get_roleid_by_rolenames (args)
            elif arr[0] == 'cancel_policy' and not is_api:
                args = args[0:2] + uiapi.cmui_get_roleid_by_rolenames (args[2:])
            buf = packet.ControlReq.make (cmd, *args).serialize ()
        except Exception, e:
            output_error (str(e))
            return False
    # console argument pass as list in data
    req_head = NetHead()
    try:
        req_head.write_msg (sock, buf)
    except Exception, e:
        output_error ("cannot send msg, %s" % str(e))
        return False
    resp_head = None
    try:
        resp_head = NetHead.read_head (sock)
    except Exception, e:
        output_error ("cannot get response, %s" % (str (e)))
        return False
    if resp_head.body_len > 0:
        resp_buf = resp_head.read_data (sock)
        resp_data = None
        try:
            resp_data = packet.deserialize (resp_buf)
        except Exception, e: 
            output_error ("invalid respond message, %s" % str (e))
            return False
        output_data (resp_data.get ('data'))
    return True

def _quit (sock):
    sock.close ()
    if not dummy:
        print "bye"
    sys.exit (0)

def usage ():
    print >> sys.stderr, """usage: %s [-e 'cmd'] [server_ip [server_port]]
    arguments:
        --api    indicate all role params input are RoleIDs, otherwise assume the params are RoleNames
        -e          execute one command in non-interact mode
        --help, -h  print this help
    Exit code indict whether command has been sucessfully processed and interact with server. 
    If exits with non-zero, error message will be output to stderr.
    Normally the output from server will be on stdout.
    """ % (sys.argv[0])

def output_data (data):
    """ if there's json module or simplejson module then dump the msg in json format,
        otherwise dump in python format.
        """
    if has_json:
        print json.dumps (data, sort_keys=True, indent=2)
    else:
        pprint.pprint (data)

def main ():
    ip = '127.0.0.1'
    port = 0
    optlist = []
    args = []
    command_line = None
    is_api = False
    global dummy
    try:
        optlist, args = getopt.gnu_getopt (sys.argv[1:], "e:h", ["help", 'api'])
    except getopt.GetoptError, e:
        print >> sys.stderr, str(e)
        return -1
    for opt, v in optlist:
        if opt in ['--help', '-h']:
            usage ()
            return 0
        elif opt == '--api':
            is_api = True
        elif opt == '-e':
            if not v:
                print >> sys.stderr, "value of '-e' must be a command"
                return -1
            dummy = True
            command_line = v

    if len (args) > 0:
        ip = args[0]
        if len (args) > 1:
            port = args[1]
    sock = _connect (ip, port)
    if not sock:
        return -1
    if dummy:
        arr = command_line.strip ("\n").strip (" ").split ()
        res = process_cmd (sock, arr, is_api or not is_master)
        sock.close ()
        if not res:
            return -1
        return 0
    while True:
        sys.stdout.write ("cmd:")
        line = sys.stdin.readline ()
        arr = line.split ()
        if line == '':
            sys.stdout.write ('\n')
            _quit (sock)
        elif line == '\n':
            continue
        if arr[0] == 'quit':
            _quit (sock)
        if not process_cmd (sock, arr, is_api):
            sock.close ()
            print >> sys.stderr, "reconnecting..."
            sock = _connect (ip, port)
            if not sock: return -1
        
if __name__ == '__main__':
    os._exit (main ())

    
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :
