#!/usr/bin/env python
# coding:utf-8

import _env
from lib.enum import Enum
from mod.packet import DownStreamPacket, PacketBase

PaxosType = Enum (PREPARE=1, PROMISE=2, PROPOSE=3, ACCEPTED=4, NACK=5)

class PaxosMsg (DownStreamPacket):

    attrs = ["paxos_type", "inst", "seq_id", "val"]
    check_attrs = ["paxos_type", "inst"]

    @classmethod
    def make (cls, server_id, paxos_type, instance, seq_id, val=None):
        self = cls ()
        self.init (server_id)
        self.data['paxos_type'] = paxos_type
        self.data['inst'] = instance
        self.data['seq_id'] = seq_id
        self.data['val'] = val
        return self

    def __str__ (self):
        msg = "%s[server_id:%s,%s]" % (self.meta_data["type"], self.meta_data["server_id"],
                self.get_time ())
        msg += " %s inst %s, seq_id %s, val %s" % (
                PaxosType._get_name (self['paxos_type']), 
                self['inst'], self['seq_id'], self['val'],
                )
        return msg

PacketBase.register(PaxosMsg)

class Instance (object):

    def __init__ (self, inst_id):
        self.inst_id = inst_id
        self.promised_seq = None
        self.accepted_val = None
        self.accepted_seq = None

        # for proposer
        self.propose_seq = None
        self.propose_val = None
        self.propose_sent = False
        self.quorum = set ()
        self.highest_accepted_seq = None
        self.highest_accepted_val = None

    def add_promise (self, remote_id, promised_seq, accepted_val):
        if self.accepted_val is None or self.highest_accepted_seq < promised_seq:
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

    def __str__ (self, is_proposer=True, is_acceptor=True, is_learner=True):
        msg = "[%s] accepted_val:%s accepted_seq:%s" % (self.inst_id, self.accepted_val, self.accepted_seq)
        if is_proposer:
            msg += " propose_seq:%s propose_val:%s highest_accepted_seq:%s highest_accepted_val:%s quorum:%s" % (
                    self.propose_seq, self.propose_val, self.highest_accepted_seq, self.highest_accepted_val, self.quorum,
                    )
        if is_acceptor:
            msg += " promised_seq:%s" % (
                 self.promised_seq,    
                    )
        return msg


class PaxosBasic (object):

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
        
    def run (self):
        pass

    def log_error (self, msg):
        self.logger.error ("server %s: %s" % (self.server_id, msg))

    def log_info (self, msg):
        self.logger.info ("server %s: %s" % (self.server_id, msg))

    def set_servers (self, proposer_ids, acceptor_ids, learner_ids):
        self.proposer_ids = set(proposer_ids)
        self.acceptor_ids = set(acceptor_ids)
        self.learner_ids = set(learner_ids)
        self.all_peers = set.union (self.proposer_ids, self.acceptor_ids, self.learner_ids)
        self.all_peers.remove (self.server_id)
        if self.server_id in self.proposer_ids:
            self.is_proposer = True
        else:
            self.is_proposer = False
        if self.server_id in self.acceptor_ids:
            self.is_acceptor = True
        else:
            self.is_acceptor = False
        if self.server_id in self.learner_ids:
            self.is_learner = True
        else:
            self.is_learner = False

    def _on_msg_to_send (self, remote_id, msg):
        raise NotImplementedError ()

    def _on_msg_recv (self, msg):
        remote_id = msg['server_id']
        paxos_type = msg['paxos_type']
        inst = msg['inst']
        seq_id = msg['seq_id']
        val = msg['val']
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
        if len (data.quorum) > len(self.acceptor_ids) / 2.0 and not data.propose_sent:
            # decide value and send propose 
            data.propose_sent = True
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
        is_new = False
        data = self.instance_get_create(inst)
        data.accepted_val = val
        if data.accepted_seq != seq_id:
            is_new = True
            data.accepted_seq = seq_id
        self.master_id = self.get_server_id_from_seq (seq_id)
        if self.master_id == self.server_id:
            self.is_master = True
            if is_new:
                self.log_info ("i am now leader")
                for server_id in self.all_peers:
                    self.send_msg (server_id, PaxosType.ACCEPTED, inst, seq_id, val)
        else:
            self.is_master = False
            self.log_info ("server %s is master, inst %s val %s seq %s" % (self.master_id, inst, seq_id, val))

    def _on_nack (self, remote_id, inst, seq_id, val):
        if not self.is_proposer:
            self.log_error ("not proposer, ignore nak from %s" % (remote_id))
            return
        #TODO what to do with val
        data = self.instance_get_create (inst)
        if seq_id > data.propose_seq: # else is old nak that can ignore
            self.start_paxos (inst, knowned_seq=seq_id)


    def send_msg (self, remote_id, paxos_type, inst, seq_id, val=None):
        msg = PaxosMsg.make (self.server_id, paxos_type, inst, seq_id, val)
        self._on_msg_to_send (remote_id, msg)

    def start_paxos (self, inst, val=None, knowned_seq=0):
        if not self.is_proposer:
            self.log_error ("not proposer, cannot start paxos")
            return
        data = self.instance_get_create (inst)
        data.propose_sent = False
        data.propose_val = val
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
