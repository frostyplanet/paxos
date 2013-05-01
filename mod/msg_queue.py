#!/usr/bin/env python

from lib.mylist import MyList
import mod.packet as packet
from lib.net_io import NetHead, send_all
from lib.conn_pool import ConnPoolError
import os
import time
import socket
import threading
from lib.persistent_queue import PersistentQueue

class MsgQueue (object):

    MIN_REG_INTERVAL = 3
    CONNECTION_RETRY = 3
    logger = None
    msgs = None
    prio_msgs = None
    _cmserver = None
    _uplink_lock = None
    _uplink_unlock = None
    _uplink_cond = None
    _recv_lock = None
    _recv_unlock = None
    _recv_cond = None
    _conn_pool = None
    _cur_conn = None
    _cur_master = None
    _cur_proxy = None
    _is_binded = 0
    _last_reg_try = None
    _uplink_th = None
    _recv_th = None
    _running_uplink = None
    _running_recv = None

    def __init__ (self, cmserver, base_path):
        self._cmserver = cmserver
        self._conn_pool = cmserver.conn_pool
        self.logger = cmserver.logger
        uplink_locker = threading.RLock ()
        recv_locker = threading.RLock ()
        self._uplink_cond = threading.Condition (uplink_locker)
        self._uplink_lock = self._uplink_cond.acquire
        self._uplink_unlock = self._uplink_cond.release
        self._recv_cond = threading.Condition (recv_locker)
        self._recv_lock = self._recv_cond.acquire
        self._recv_unlock = self._recv_cond.release
        self.unreliable_uplinkmsgs = MyList ()
        if not os.path.exists (base_path):
            os.makedirs (base_path)
        self.reliable_uplinkmsgs = PersistentQueue (os.path.join (base_path, "uplink_msg.gdbm"))
        self.received_msgs = PersistentQueue (os.path.join (base_path, "recieved_msg.gdbm"))
        self.prio_msgs = MyList ()
        self._running_uplink = False
        self._running_recv = False

    def start_uplink (self):
        if self._running_uplink:
            return
        assert self._uplink_th is None
        self._running_uplink = True
        self._uplink_th = threading.Thread (target=self._worker_uplink)
        self._uplink_th.setDaemon (1)
        self._uplink_th.start ()

    def start_recv (self, process_func):
        if self._running_recv:
            return
        assert self._recv_th is None
        self._running_recv = True
        self._recv_th = threading.Thread (target=self._worker_recv, args=(process_func,))
        self._recv_th.setDaemon (1)
        self._recv_th.start ()

    def get_status (self):
        self._uplink_lock ()
        unreliable_uplinkmsgs = map (str, self.unreliable_uplinkmsgs)
        queued_uplink_count = len (self.reliable_uplinkmsgs.get_queued_ids ())
        self._uplink_unlock ()
        self._recv_lock ()
        queued_recv_count = len (self.received_msgs.get_queued_ids ())
        self._recv_unlock ()
        return (unreliable_uplinkmsgs, queued_uplink_count, queued_recv_count)

    def add_uplinkmsg (self, p):
        assert isinstance (p, packet.UplinkPacket)
        self._uplink_lock ()
        if p.can_retry:
            self.reliable_uplinkmsgs.append (p.serialize ())
        else:
            self.unreliable_uplinkmsgs.append (p)
        self._uplink_cond.notify ()
        self._uplink_unlock ()

    def add_prio_uplinkmsg (self, p, cb):
        self._uplink_lock ()
        self.prio_msgs.append ((p, cb))
        self._uplink_cond.notify ()
        self._uplink_unlock ()

    def add_recvmsg (self, p):
        assert isinstance (p, packet.UplinkPacket)
        self._recv_lock ()
        self.received_msgs.append (p.serialize ())
        self._recv_cond.notify ()
        self._recv_unlock ()

    def _close_conn (self):
        if self._cur_conn:
            self._conn_pool.close_conn (self._cur_conn)
            self._cur_conn = None

    def _get_conn (self):
        if self._cur_conn is None and self._is_binded == 1:
            if not self._cur_proxy:
                return None
            try:
                self._cur_conn = self._conn_pool.get_conn (
                        (self._cur_proxy, self._cmserver._default_port), retry=self.CONNECTION_RETRY)
            except ConnPoolError, e:
                self.logger.warn ("cannot connect to %s, %s" % (
                    (self._cur_proxy, self._cmserver._default_port), str(e)))
                return None
        return self._cur_conn

    def _worker_uplink (self):
        self._uplink_lock ()
        while self._running_uplink:
            reliable_uplink_p = self.reliable_uplinkmsgs.get_head ()
            if self.prio_msgs:
                p_n_cb = self.prio_msgs.popleft ()
                self._uplink_unlock ()
                try:
                    p_n_cb[1] (p_n_cb[0])
                except Exception, e:
                    self.logger.exception ("uncatched exception %s" % (str (e)))
                self._uplink_lock ()
                continue
            elif self.unreliable_uplinkmsgs:
                p = self.unreliable_uplinkmsgs.popleft ()
                self._uplink_unlock ()
                res = self._send_unreliable_msg (p)
                self._uplink_lock ()
                if not res: # if msg send fail, left the confirm_proxy() call to wake me up
                    self._close_conn ()
                    self._uplink_cond.wait ()
            elif reliable_uplink_p:
                self._uplink_unlock ()
                res = self._send_reliable_msg (reliable_uplink_p)
                self._uplink_lock ()
                if res:
                    self.reliable_uplinkmsgs.pop_head ()
                else:
                    self._close_conn ()
                    self._uplink_cond.wait ()
            else: # if no msg pending, close the connection
                self._close_conn ()
                self._uplink_cond.wait ()
        self._uplink_unlock ()
        return

    def _worker_recv (self, proc_func):
        assert callable (proc_func)
        res = None
        self._recv_lock ()
        while self._running_recv:
            recv_p = self.received_msgs.get_head ()
            if recv_p:
                self._recv_unlock ()
                try:
                    res = proc_func (recv_p)
                except Exception, e:
                    self.logger.exception (e)
                if res:
                    self._recv_lock ()
                    self.received_msgs.pop_head ()
                else:
                    time.sleep (1)
                    self._recv_lock ()
            else:
                self._recv_cond.wait ()
        self._recv_unlock ()

    def _send_unreliable_msg (self, data):
        up_conn = self._get_conn ()
        if not up_conn:
            self.register ()
            return False
        try:
            buf = packet.serialize (data)
            send_all (up_conn.sock, buf)
            NetHead.read_head (up_conn.sock)
            self._cmserver._log_cmd_send (self._cur_proxy, data, True)
            return True
        except (ValueError, socket.error, packet.PacketDataError), e: 
            self._cmserver._log_cmd_send (self._cur_proxy, data, False, will_retry=False, error=e)
            self.register ()
            return False

    def _send_reliable_msg (self, buf):
        up_conn = self._get_conn ()
        if not up_conn:
            self.register ()
            return False
        data = packet.PacketBase.deserialize (buf) # assume packet is correct
        try:
            NetHead ().write_msg (up_conn.sock, buf)
            NetHead.read_head (up_conn.sock)
            self._cmserver._log_cmd_send (self._cur_proxy, data, True)
            return True
        except (socket.error, packet.PacketDataError), e: 
            self._cmserver._log_cmd_send (self._cur_proxy, data, False, will_retry=True, error=e)
            self.register ()
            return False

    def _register (self, data):
        if self._is_binded == 0:
            if self._last_reg_try and time.time () - self._last_reg_try <= self.MIN_REG_INTERVAL:  #does not try more than every sec
                self.logger.debug ("skip register")
                return
        self._is_binded = 0
        self._last_reg_try = time.time ()
        self._close_conn ()
        if self._cmserver.hostname is None:
            self.logger.warn ("no hostname, skip register")
            return
        self.logger.info ("start register", bt_level=1)
        ips = []
        _masters = self._cmserver._masters
        if isinstance (_masters, str):
            _ip, _alias, ips = socket.gethostbyname_ex (_masters)
        else:
            ips = _masters
        buf = packet.serialize (packet.BindReq.make (self._cmserver.hostname))
        for host in ips:
            if host == self._cmserver.hostname:
                continue
            try:
                conn = self._conn_pool.get_conn ((host, self._cmserver._default_port), timeout=10)
                send_all (conn.sock, buf)
                NetHead.read_head (conn.sock)
                self._conn_pool.close_conn (conn)
                self.logger.info ("BindReq sent to %s" % (host))
                return
            except ConnPoolError, e:
                self.logger.warn ("cannot connect to %s, %s" % (host, str(e)))
                continue
            except (ValueError, socket.error, packet.PacketDataError), e: 
                self._conn_pool.close_conn (conn)
                self.logger.exception ("cannot register to %s, %s" % (host, str(e)))
                continue
        self.logger.error ("no proxy can be connected to")

    def _confirm_proxy (self, data):
        (proxy_addr, master_addr) = data
        self._close_conn ()
        try:
            conn = self._conn_pool.get_conn ((proxy_addr, self._cmserver._default_port), retry=self.CONNECTION_RETRY, timeout=20)
            data = packet.BindAck.make (self._cmserver.hostname,
                master_addr, proxy_addr, self._cmserver.version)
            buf = packet.serialize (data)
            send_all (conn.sock, buf)
            NetHead.read_head (conn.sock)
            self._cur_conn = conn
            self._cur_proxy = proxy_addr
            self._cur_master = master_addr
            self._is_binded = 1
            self.logger.info ("set proxy_addr=%s, master_addr=%s" % (proxy_addr, master_addr))
            return
        except ConnPoolError, e:
            self.logger.error ("cannot establish proxy as %s, %s" % (proxy_addr, str(e)))
        except (ValueError, socket.error, packet.PacketDataError), e: 
            self._conn_pool.close_conn (conn)
            self.logger.error ("cannot establish proxy as %s, %s" % (proxy_addr, str(e)))

    def confirm_proxy (self, proxy_addr, master_addr):
        self.add_prio_uplinkmsg ((proxy_addr, master_addr), self._confirm_proxy)
     
    def register (self):
        self.add_prio_uplinkmsg ("reg", self._register)

    def stop (self):
        if self._running_uplink:
            assert self._uplink_th
            self._running_uplink = False
            self._uplink_lock ()
            self._uplink_cond.notifyAll () # get all sleeping worker up
            self._uplink_unlock ()
            self._uplink_th.join () 
            self.reliable_uplinkmsgs.close ()
            self.reliable_uplinkmsgs = None
            self.received_msgs.close ()
            self.received_msgs = None
            self._uplink_th = None
        if self._running_recv:
            assert self._recv_th
            self._running_recv = False
            self._recv_lock ()
            self._recv_cond.notifyAll ()
            self._recv_unlock ()
            self._recv_th.join ()
            self._recv_th = None
        

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :
