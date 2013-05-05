#!/usr/bin/env python
# coding:utf-8

import _env
from mod.paxos import PaxosType, PaxosBasic, PaxosMsg
from lib.log import Log
import config
import unittest
import collections
import threading
import time
import random

class PaxosThreadedMock (PaxosBasic):

    def __init__ (self, server_id, logger, peers):
        PaxosBasic.__init__ (self, server_id, logger)
        self.peers = peers
        self.in_msg_queue = collections.deque ()
        self.lock = threading.Lock ()
        self.cond = threading.Condition(self.lock)
        self.is_running = False
        self.th = None

    def start (self):
        self.is_running = True
        self.th = threading.Thread (target=self.run)
        self.th.setDaemon (1)
        self.th.start ()

    def wait (self):
        self.th.join ()

    def _on_msg_to_send (self, remote_id, msg):
        self.logger.info ("%s->%s: %s" % (self.server_id, remote_id, str(msg)))
        try:
            self.peers[remote_id].recv_msg (msg)
        except Exception, e:
            self.logger.exception (e)

    def accepted_callback (self, inst, val):
        self.is_running = False
        self.cond.acquire ()
        self.cond.notify ()
        self.cond.release ()
        self.logger.info ("%s callback inst %s val %s" % (self.server_id, inst, val))

    def cleanup (self):
        self.in_msg_queue = collections.deque ()

    def run (self):
        self.cond.acquire ()
        while self.is_running:
            try:
                msg = self.in_msg_queue.popleft ()
                self.cond.release ()
                self._on_msg_recv (msg)
                self.cond.acquire ()
            except IndexError:
                self.cond.wait ()
        self.cond.release ()
        self.logger.info ("%s exiting" % (self.server_id))

    def recv_msg (self, msg):
        self.cond.acquire ()
        self.in_msg_queue.append (msg)
        self.cond.notify ()
        self.cond.release ()


class TestPaxosBasic (unittest.TestCase):

    logger = Log ("test", config=config)
    logger_result = Log ("result", config=config)
    start_time = None
    all_paxos_time = []


    def set_servers (self, proposers, acceptors, learners):
        all_servers = set.union(set(proposers), set(acceptors), set (learners))

        for i in xrange (len(all_servers)):
            server = self.paxos[i] = PaxosThreadedMock (i, self.logger, self.paxos)
            server.set_servers (proposers, acceptors, learners)
    
    def start (self):
        for server in self.paxos.values ():
            server.start ()

    def wait (self, inst):
        for server in self.paxos.values ():
            server.wait ()
        for server in self.paxos.values ():
            server.cleanup ()
        self.logger.info ("done inst %s" % (inst))


    def start_elect (self, ids, inst):
        self.start_time = time.time ()
        val = random.randint (1, 65535)
        for server_id in ids:
            self.paxos[server_id].start_paxos (inst, val)
        
    def collect_result (self, run, inst):
        try:
            paxos_time = []
            master_id = None
            for server in self.paxos.values ():
                data = server.instance_get_create (inst)
                self.assert_ (server.master_id is not None)
                if master_id is None:
                    master_id = server.master_id
#                else:
#                    self.assertEqual (master_id, server.master_id)
                self.assertEqual (data.error, 0)
                if data.paxos_time:
                    paxos_time.append (data.paxos_time)
            max_time = max (paxos_time)
            self.logger_result.info ("test %s inst %s: master is %s, longest proposer run %s sec" % (run, inst, master_id, max_time))
            self.all_paxos_time.append (max_time)
        except Exception, e:
            self.logger_result.exception ("test %s, inst %s: %s" % (run, inst, str(e)))
            self.print_state (inst)
            self.fail (inst)


    def print_state (self, i):
        for server in self.paxos.values ():
            self.logger.info ("________%s__instance %s_________" % (server.server_id, i))
            data = server.instance_get_create (i)
            self.logger.info (data.__str__ (server.is_proposer, server.is_acceptor, server.is_learner))

    def setUp (self):
        self.paxos = dict ()
        self.msg_queue = collections.deque ()

    def test_dueling (self):
        self.logger.warn ("\n========test2 dueling paxos========")
        proposers = [0, 1, 2]
        acceptors = [2, 3, 4, 5]
        learners = [5, 6, 7]
        self.set_servers (proposers, acceptors, learners)
        for inst in xrange (0, 1000):
            self.start ()
            self.start_elect (proposers, inst)
            self.wait (inst)
            self.collect_result (0, inst)
        print "max_time", max(self.all_paxos_time)
       

if __name__ == '__main__':
    unittest.main ()



# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :
