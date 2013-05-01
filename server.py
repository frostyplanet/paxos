#!/usr/bin/env python
# coding:utf-8

import time
import errno
import os
from lib.io_poll import get_poll
from lib.log import Log
from lib.socket_engine import TCPSocketEngine, Connection, ConnectNonblockError, ReadNonBlockError, WriteNonblockError
from lib.job_queue import JobQueue
from lib.timer_events import TimerEvents
import signal
import sys
import mod.packet as packet
from lib.net_io import NetHead
import config
import lib.daemon as daemon

class PaxosServer (object):

    def __init__ (self, logger, _id, addr):
        self.logger = logger
        self._bind_addr = addr
        self.server_id = _id
        self.is_running = False
        self.server = TCPSocketEngine (get_poll(), debug=True, is_blocking=False)
        self.server.set_timeout (10, 1800)
        self.server.set_logger (self.logger)
        self._timers = TimerEvents (time.time, logger)
        self.passive_sock = None
        self._handles = {
                packet.StatusReq.__name__: self._recv_status_req
                }
        self._empty_head_ack = NetHead ().pack ()
        print "init"

    def start (self):
        if self.is_running:
            return
        self.is_running = True
        self._timers.start ()
        self.passive_sock = self.server.listen_addr (self._bind_addr,
                readable_cb=self._read_msg, readable_cb_args=(self._server_nexus, None, ),
                backlog=50)
        self.logger.info ("%s started" % (self.server_id))


    def loop (self):
        while self.is_running:
            try:
                self.server.poll (timeout=100)
            except KeyboardInterrupt, e:
                raise e
            except Exception, e:
                self.logger.exception_ex (e)
        self.logger.info ("%s all stopped" % (self.server_id))

    def stop (self):
        if not self.is_running:
            return
        self._timers.stop ()
        self.server.unlisten (self.passive_sock)
        self.is_running = False

    def send_ack (self, conn, data):
        def __on_send_ack_err (conn, *args):
            self.logger.error ("cannot send ack of %s to %s, %s" % (str(data), str(conn.peer), str(conn.error)))
            return
        self.server.write_unblock (conn, self._empty_head_ack, self.server.watch_conn, __on_send_ack_err)

    def _server_nexus (self, conn, data):
        """ the nexus of handling message receiving and delivery """
        try:
            if isinstance (data, packet.DownStreamPacket):
                self.send_ack (conn, data)
                self._server_handler (conn, data)
                return
            elif isinstance (data, packet.DirectPacket):
                # for DirectPacket, there's no guarentee the 'client_name' attr will present
                self._log_cmd_recv (conn.peer, data)
                self._server_handler (conn, data)
            else:
                self.logger.error ("from peer %s recv unrecognized data" % (str(conn.peer)))
            return
        except Exception, e:
            self.logger.exception_ex (str (e))
            self.server.close_conn (conn)


    def _server_handler (self, conn, data):
        """ get the right handler to do its job for a message """
        try:
            handle_name = data['type']
            handle = self._handles.get (handle_name)
            #calling packet handle
            if not callable (handle):
                self.logger.error ("from peer %s, no such handle for %s" % (str(conn.peer), str(data)))
                self.server.close_conn (conn)
                return
            handle (conn, data)
        except Exception, e:
            self.logger.exception_ex ("from peer %s, %s" % (str(conn.peer), str (e)))
            self.server.close_conn (conn)

    def _read_msg (self, conn, msg_handler, err_handler=None):
        """ the server param must be self.server just to conform with socketengine callback requirement.
            msg_handler param: server, conn, data # if head's body_len==0, data will be None
            err_handler param: server, conn, ...uncertain args...  #error is put in conn.error
            """
        def __err_wrapper (conn, *args):
            if callable (err_handler):
                err_handler (conn)
            return
        self.server.read_unblock (conn, NetHead.size, self._on_recv_head, __err_wrapper,
                cb_args=(msg_handler, __err_wrapper))


    def _on_recv_head (self, conn, msg_handler, err_handler=None):
        """ msg_handler param: conn, data # if head's body_len==0, data will be None
            err_handler param: conn, ...uncertain args...  #error is put in conn.error
            """
        assert callable(msg_handler)
        head = None
        try:
            head = NetHead.unpack (conn.get_readbuf ())
        except Exception, e:
            self.logger.error ("from peer %s, %s" % (str(conn.peer), str (e)))
            if callable (err_handler):
                conn.error = e
                err_handler (conn)
            self.server.close_conn (conn)
            return
        if head.body_len == 0:
            msg_handler (conn, None)
        else:
            self.server.read_unblock (conn, head.body_len, self._on_recv_msg, err_handler, cb_args=(msg_handler, err_handler))
        return

    def _on_recv_msg (self, conn, msg_handler, err_handler=None):
        """ msg_handler param: conn, data
            err_handler param: conn, ...uncertain args... # error is put in conn.error 
            """
        assert callable (msg_handler)
        data = None
        try:
            data = packet.deserialize (conn.get_readbuf ())
            msg_handler (conn, data)
        except packet.PacketDataError, e:
            self.logger.exception ("from peer %s, %s" % (str(conn.peer), str(e)))
            if callable (err_handler):
                conn.error = e
                err_handler (conn)
            self.server.close_conn (conn)
        return


    def _connect_and_send (self, addr, buf, ok_callback, err_callback):
        """ ok_callback param: conn,
            err_callback param: e,
            recv a NetHead which's body_len == 0 after buf is sent
            """
        assert isinstance (buf, str)
        assert isinstance (addr, tuple) and len (addr) == 2
        assert callable (ok_callback)
        def __on_read_head (conn):
            try:
                NetHead.unpack (conn.get_readbuf ())
                return ok_callback (conn)
            except ValueError, e:
                self.server.close_conn (conn)
                if callable (err_callback):
                    return err_callback (e)
        def __on_write (conn):
            return self.server.read_unblock (conn, NetHead.size, __on_read_head, __on_err)
        def __on_err (conn):
            if callable (err_callback):
                return err_callback (conn.error)
        def __on_conn (sock):
            return self.server.write_unblock (Connection (sock), buf, __on_write, __on_err)
        def __on_conn_err (e):
            if callable (err_callback):
                err_callback (e)
        self.server.connect_unblock (addr, __on_conn, __on_conn_err)
        return

    def _log_cmd_send (self, remote_addr, data, is_suc, error=""):
        dest = "to %s" % str(remote_addr)
        if is_suc:
            self.logger.info ("server%s: packet %s sent %s" % (self.server_id, str(data), dest),  bt_level=1)
        else:
            self.logger.error ("server%s: packet %s cannot be sent %s, %s" % (self.server_id, str(data), dest, str(error)), bt_level=1)
        return

    def _log_cmd_recv (self, remote_addr, data):
        self.logger.info ("packet %s recv from %s" % (str(data), str(remote_addr)), bt_level=1)
        return

    def _send_msg_down (self, remote_addr, data):
        assert isinstance (data, packet.DownStreamPacket)
        buf = packet.serialize (data)

        def __on_err (e):
            self._log_cmd_send (remote_addr, data, False, error=e)
            return
        def __on_send_suc (conn):
            self._log_cmd_send (remote_addr, data, True)
            self.server.close_conn (conn)
            return
        self._connect_and_send (remote_addr, buf, __on_send_suc, __on_err)


    def _send_direct_resp (self, conn, data):
        def __on_ok (conn):
            self._log_cmd_send (conn.peer, data, True)
            self.server.watch_conn (conn)
            return
        def __on_err (conn):
            self._log_cmd_send (conn.peer, data, False, error=str(conn.error))
            return
        buf = packet.serialize (data)
        self.server.write_unblock (conn, buf, __on_ok, __on_err)

    def _recv_status_req (self, conn, data):
        resp = packet.StatusResp.make (self.server_id, "")
        self._send_direct_resp (conn, resp)


stop_signal_flag = False

def main ():
    server_id = int(sys.argv[2])
    logger = Log ("server%s" % (server_id), config=config) # to ensure log is permitted to write
    c = PaxosServer (logger, server_id, config.SERVERS[server_id])

    def exit_sig_handler (sig_num, frm):
        global stop_signal_flag
        if stop_signal_flag:
            return
        stop_signal_flag = True
        c.stop ()
        return
    c.start ()
    signal.signal (signal.SIGTERM, exit_sig_handler)
    signal.signal (signal.SIGINT, exit_sig_handler)
    c.loop ()
    return

def usage ():
#    print "usage:\t%s star/stop/restart\t#manage forked daemon" % (sys.argv[0])
    print "\t%s run ID \t\t# run without daemon, for test purpose" % (sys.argv[0])
    os._exit (1)

if __name__ == '__main__':
    if len (sys.argv) <= 2:
        usage ()
    else:
        logger = Log ("daemon", config=config) # to ensure log is permitted to write
        pid_file = "paxos_server.pid"
        mon_pid_file = "paxos_server_mon.pid"
        action = sys.argv[1]
        daemon.cmd_wrapper (action, main, usage, logger, config.log_dir, config.RUN_DIR, pid_file, mon_pid_file)



# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :
