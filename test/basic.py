#!/usr/bin/env python
# coding:utf-8

import _env
from mod.paxos import PaxosType, PaxosBasic, PaxosMsg
from lib.log import Log
import config
import unittest

class PaxosMock(PaxosBasic):

    def __init__(self, server_id, logger, msg_queue):
        PaxosBasic.__init__(self, server_id, logger)
        self.msg_queue = msg_queue

    def _on_msg_to_send(self, remote_id, msg):
        self.msg_queue.append((self.server_id, remote_id, msg))

    def recv_msg(self, msg):
        self._on_msg_recv(msg)


class TestPaxosBasic(unittest.TestCase):

    logger = Log("test", config=config)

    def _proc_msg(self):
        while True:
            data = None
            try:
                data = self.msg_queue.pop(0)
            except IndexError:
                return
            from_server, to_server, msg = data
            self.logger.info("%s->%s: %s" % (from_server, to_server, str(msg)))
            self.paxos[to_server].recv_msg(msg)

    def set_servers(self, proposers, acceptors, learners):
        all_servers = set.union(set(proposers), set(acceptors), set(learners))
        for i in xrange(len(all_servers)):
            server = self.paxos[i] = PaxosMock(i, self.logger, self.msg_queue)
            server.set_servers(proposers, acceptors, learners)


    def print_state(self, i):
        for server in self.paxos.values():
            self.logger.info("________%s__instance %s_________" % (server.server_id, i))
            data = server.instance_get_create(i)
            self.logger.info(data.__str__(server.is_proposer, server.is_acceptor, server.is_learner))
            self.assertEqual(data.error, 0)

    def setUp(self):
        self.paxos = dict()
        self.msg_queue = []

    def test_1(self):
        self.logger.warn("\n========test1=========")
        proposers = [0, 1]
        acceptors = [2, 3, 4]
        learners = [5]
        self.set_servers(proposers, acceptors, learners)
        self.paxos[0].start_paxos(0, 12345)
        self._proc_msg()
        self.print_state(0)
        self.assert_(self.paxos[0].is_master)

    def test_2_dueling(self):
        self.logger.warn("\n========test2 dueling paxos========")
        proposers = [0, 1]
        acceptors = [2, 3, 4]
        learners = [5]
        self.set_servers(proposers, acceptors, learners)
        self.paxos[0].start_paxos(0, 12345)
        self.paxos[1].start_paxos(0, 2345)
        self._proc_msg()
        self.print_state(0)
        self.assert_(self.paxos[0].is_master or self.paxos[1].is_master)
        self.assert_(not(self.paxos[0].is_master and self.paxos[1].is_master))
       

if __name__ == '__main__':
    unittest.main()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :
