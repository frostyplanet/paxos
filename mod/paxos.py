#!/usr/bin/env python
# coding:utf-8

import _env
from lib.enum import Enum
from mod.packet import PaxosMsg

PaxosRole = Enum (PROPOSER=1, ACCEPTOR=2, LEARNER=3)
PaxosType = Enum (PREPARE=1, PROMISE=2, PROPOSE=3, ACCEPTED=4, NACK=5)


class Instance (object):

    def __init__ (self, inst_id):
        self.inst_id = inst_id
        self.promised_seq = None
        self.accepted_val = None
        self.accepted_seq = None

        # for proposer
        self.propose_seq = None
        self.propose_val = None
        self.quorum = set ()
        self.highest_accepted_seq = None
        self.highest_accepted_val = None

    def add_promise (self, remote_id, promised_seq, accepted_val):
        if self.accepted_val is not None and self.highest_accepted_seq < promised_seq:
            self.highest_accepted_val = accepted_val
            self.highest_accepted_seq = promised_seq
        self.quorum.add (remote_id)

    def increase_seq (self, server_id, n, max_seq=0):
        if self.propose_seq is None:
            self.propose_seq = 0
        i = max(max_seq, self.propose_seq) / n
        while True:
            seq_id = i * n + server_id
            if seq_id < max_seq or seq_id <= self.propose_seq:
                i += 1
            else:
                self.propose_seq = seq_id
                return seq_id


class Paxos (object):

    def __init__ (self, server_id, logger):
        self.logger = logger 
        self.state = {}
        self.max_inst = 0
        self.is_master = False
        self.is_learner = False
        self.is_acceptor = False
        self.is_proposer = False
        self.server_id = server_id
        self.master_id = None
        self.proposer_ids = set ()
        self.acceptor_ids = set ()
        self.learner_ids = set ()
        self.all_peers = set ()

    @property
    def server_numbers (self):
        return len (self.all_peers) + 1
    
    def instance_get_create (self, inst_id):
        i = self.state.get (inst_id)
        if not i:
            self.state[inst_id] = Instance (inst_id)
            i = self.state[inst_id]
        return i

    def get_server_id_from_seq (self, seq):
        return seq % self.server_numbers
        

    def log_error (self, msg):
        self.logger.error ("server %s: %s" % (self.server_id, msg))

    def log_info (self, msg):
        self.logger.info ("server %s: %s" % (self.server_id, msg))

    def set_servers (self, proposer_ids, acceptor_ids, learner_ids):
        self.proposer_ids = set(proposer_ids)
        self.acceptor_ids = set(acceptor_ids)
        self.learner_ids = set(learner_ids)
        self.all_peers = self.proposer_ids + self.acceptor_ids + self.learner_ids
        self.all_peers.remove (self.server_id)
        if self.server_id in self.proposer_ids:
            self.is_proposer = True
        if self.server_id in self.acceptor_ids:
            self.is_acceptor = True
        if self.server_id in self.learner_ids:
            self.is_learner = True

    def _on_msg_send (self, remote_id, msg):
        raise NotImplementedError ()

    def _on_msg_recv (self, msg):
        remote_id = msg.data['server_id']
        paxos_type = msg.data['paxos_type']
        inst = msg.data['inst']
        seq_id = msg.data['seq_id']
        val = msg.data['val']
        if inst > self.max_inst:
            self.max_inst = inst
        if paxos_type == PaxosType.PREPARE:
            self._on_prepare (remote_id, inst, seq_id)
        elif paxos_type == PaxosType.PROMISE:
            self._on_promise (remote_id, inst, seq_id, val)
        elif paxos_type == PaxosType.PROPOSE:
            self._on_propose (remote_id, inst, seq_id, val)
        elif paxos_type == PaxosType.ACCEPTED:
            self._on_accepted (remote_id, inst, seq_id, val)
        elif paxos_type == PaxosType.NACK:
            self._on_nack (remote_id, inst, seq_id, val)

    def _on_prepare (self, remote_id, inst, seq_id):
        if not self.is_acceptor:
            self.log_error ("not acceptor, ignore prepare from %s" % (remote_id))
            return
        data = self.instance_get_create (inst)
        if data.promised_seq > seq_id:
            self.send_msg (remote_id, PaxosType.NACK, inst, data.promised_seq, data.accepted_val)
            return
        data.promised_seq = seq_id
        if data.accepted_val is not None:
            self.send_msg (remote_id, PaxosType.PROMISE, inst, data.accepted_seq, data.accepted_val)
        else:
            self.send_msg (remote_id, PaxosType.PROMISE, inst, seq_id, None)
        return


    def _on_promise (self, remote_id, inst, seq_id, val):
        if not self.is_proposer:
            self.log_error ("not proposer, ignore promise from %s" % (remote_id))
            return
        data = self.instance_get_create (inst)
        data.add_promise (remote_id, seq_id, val)
        if len (data.quorum) > len(self.acceptor_ids) / 2.0:
            # decided
            seq_id = data.propose_seq
            val = None
            if data.highest_accepted_val is not None:
                val = data.accepted_val = data.highest_accepted_val
            else:
                val = data.propose_val
            for server_id in self.acceptor_ids:
                self.send_msg (server_id, PaxosType.PROPOSE, inst, seq_id, val)


    def _on_propose (self, remote_id, inst, seq_id, val):
        if not self.is_acceptor:
            self.log_error ("not acceptor, ignore propose from %s" % (remote_id))
            return
        data = self.instance_get_create (inst)
        if data.promised_seq > seq_id:
            self.send_msg (remote_id, PaxosType.NACK, inst, data.promised_seq, data.accepted_val)
            return
        data.promised_seq = seq_id
        data.accepted_seq = seq_id
        data.accepted_val = val
        self.send_msg (remote_id, PaxosType.ACCEPTED, inst, seq_id, val)


    def _on_accepted (self, remote_id, inst, seq_id, val):
        data = self.instance_get_create(inst)
        data.accepted_val = val
        data.accepted_seq = seq_id
        if self.get_server_id_from_seq (seq_id) == self.server_id:
            self.log_info ("i am now leader")
            for server_id in self.all_peers:
                self.send_msg (server_id, PaxosType.ACCEPTED, inst, seq_id, val)

    def _on_nack (self, remote_id, inst, seq_id, val):
        if not self.is_proposer:
            self.log_error ("not proposer, ignore nak from %s" % (remote_id))
            return
        #TODO what to do with val
        self.start_paxos (inst, seq_id)


    def send_msg (self, remote_id, paxos_type, inst, seq_id, val=None):
        msg = PaxosMsg.make (self.server_id, paxos_type, inst, seq_id, val)
        self._on_msg_send (remote_id, msg)

    def start_paxos (self, inst, knowned_seq=0):
        if not self.is_proposer:
            self.log_error ("not proposer, cannot start paxos")
            return
        data = self.instance_get_create (inst)
        data.increase_seq (self.server_id, self.server_numbers, knowned_seq)
        for server_id in self.acceptor_ids:
            self.send_msg (server_id, PaxosType.PREPARE, inst, data.propose_seq, data.accepted_val)

        
        

def test ():
    data = Instance (0)
    print data.increase_seq(0, 3), data.propose_seq
    print data.increase_seq(0, 3), data.propose_seq
    print data.increase_seq(9, 3), data.propose_seq
    print data.increase_seq(0, 3), data.propose_seq

if __name__ == '__main__':
    test ()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :
