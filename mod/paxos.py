#!/usr/bin/env python
# coding:utf-8

# @file socket_egine.py 
# @author frostyplanet@gmail.com

import _env
from lib.enum import Enum
from mod.packet import DownStreamPacket, PacketBase
import time

PaxosType = Enum(PREPARE=1, PROMISE=2, PROPOSE=3, ACCEPTED=4, LEARN=5, NACK=6)
PaxosProposerState = Enum(NEW=0, PREPARE=1, PROPOSE=3, ACCEPTED=4)

class PaxosMsg(DownStreamPacket):

    attrs = ["paxos_type", "inst", "seq_id", "val"]
    check_attrs = ["paxos_type", "inst"]

    @classmethod
    def make(cls, server_id, paxos_type, instance, seq_id, val=None):
        self = cls()
        self.init(server_id)
        self.data['paxos_type'] = paxos_type
        self.data['inst'] = instance
        self.data['seq_id'] = seq_id
        self.data['val'] = val
        return self

    def __str__(self):
        msg = "%s[server_id:%s,%s]" % (self.meta_data["type"], self.meta_data["server_id"],
                self.get_time())
        msg += " %s inst %s, seq_id %s, val %s" % (
                PaxosType._get_name(self['paxos_type']), 
                self['inst'], self['seq_id'], self['val'],
                )
        return msg

PacketBase.register(PaxosMsg)

class Instance(object):

    def __init__(self, inst_id, logger):
        self.inst_id = inst_id
        self.promised_seq = None
        self.accepted_val = None
        self.accepted_seq = None
        self.logger = logger
        self.error = 0

        # for proposer
        self.propose_seq = None
        self.propose_val = None
        self.propose_state = PaxosProposerState.NEW
        self.propose_last_ts = time.time()
        self.start_ts = 0
        self.paxos_time = 0  # only calculate on the one become master
        self.retry = False
        self.promised_quorum = set()
        self.accepted_quorum = set()
        self.highest_accepted_seq = None
        self.highest_accepted_val = None

    def add_promise(self, remote_id, promised_seq, accepted_val):
        if accepted_val is not None:
            if self.accepted_val is not None and self.accepted_val != accepted_val:
                self.logger.error("conflict detected: inst %s %s(seq %s) != %s(seq %s)" % (
                    self.inst_id, self.accepted_val, self.highest_accepted_seq,
                    accepted_val, promised_seq))
                self.error += 1
            if promised_seq > self.propose_seq:
                self.logger.error("error detected: promised_seq %s > propose_seq %s" % (promised_seq, self.propose_seq))
                self.error += 1
            if self.accepted_val is None or self.highest_accepted_seq < promised_seq:
                self.highest_accepted_val = accepted_val
                self.highest_accepted_seq = promised_seq
        self.promised_quorum.add(remote_id)

    def increase_seq(self, server_id, n, max_seq=0):
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

    def __str__(self, is_proposer=True, is_acceptor=True, is_learner=True):
        msg = "[%s] accepted_val:%s accepted_seq:%s" % (self.inst_id, self.accepted_val, self.accepted_seq)
        if is_proposer:
            msg += " propose_seq:%s propose_val:%s highest_accepted_seq:%s highest_accepted_val:%s promised_quorum:%s accepted_quorum:%s paxos_time:%s, start_ts:%s" % (
                    self.propose_seq, self.propose_val, self.highest_accepted_seq, self.highest_accepted_val, self.promised_quorum, self.accepted_quorum,
                    self.paxos_time, self.start_ts
                    )
        if is_acceptor:
            msg += " promised_seq:%s" % (
                 self.promised_seq, 
                    )
        return msg


class PaxosBasic(object):

    def __init__(self, server_id, logger):
        self.logger = logger 
        self.state = {}
        self.max_inst = 0
        self.is_master = False
        self.is_learner = False
        self.is_acceptor = False
        self.is_proposer = False
        self.server_id = server_id
        self.master_id = None
        self.proposer_ids = set()
        self.acceptor_ids = set()
        self.learner_ids = set()
        self.quorum_ids = set()
        self.all_peers = set()

    @property
    def server_numbers(self):
        return len(self.all_peers) + 1
    
    def instance_get_create(self, inst_id):
        i = self.state.get(inst_id)
        if not i:
            self.state[inst_id] = Instance(inst_id, self.logger)
            i = self.state[inst_id]
        return i

    def get_server_id_from_seq(self, seq):
        return seq % self.server_numbers
        
    def check_timeout(self, inst, timeout):
        if not self.is_proposer:
            return
        data = self.instance_get_create(inst)
        if data.start_ts and data.propose_state != PaxosProposerState.ACCEPTED and \
                time.time() - data.start_ts > timeout:
            self.start_paxos(inst)


    def log_error(self, msg):
        self.logger.error("server %s: %s" % (self.server_id, msg))

    def log_info(self, msg):
        self.logger.info("server %s: %s" % (self.server_id, msg))

    def set_servers(self, proposer_ids, acceptor_ids, learner_ids):
        self.proposer_ids = set(proposer_ids)
        self.acceptor_ids = set(acceptor_ids)
        self.learner_ids = set(learner_ids)
        self.all_peers = set.union(self.proposer_ids, self.acceptor_ids, self.learner_ids)
        self.all_peers.remove(self.server_id)
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
        if self.is_proposer:
            self.quorum_ids = set(self.acceptor_ids)
            if self.is_acceptor:
                self.quorum_ids.remove(self.server_id)

    def _on_msg_to_send(self, remote_id, msg):
        raise NotImplementedError()

    def _on_msg_recv(self, msg):
        remote_id = msg['server_id']
        paxos_type = msg['paxos_type']
        inst = msg['inst']
        seq_id = msg['seq_id']
        val = msg['val']
        if inst > self.max_inst:
            self.max_inst = inst
        if paxos_type == PaxosType.PREPARE:
            self._on_prepare(remote_id, inst, seq_id)
        elif paxos_type == PaxosType.PROMISE:
            self._on_promise(remote_id, inst, seq_id, val)
        elif paxos_type == PaxosType.PROPOSE:
            self._on_propose(remote_id, inst, seq_id, val)
        elif paxos_type == PaxosType.ACCEPTED:
            self._on_accepted(remote_id, inst, seq_id, val)
        elif paxos_type == PaxosType.LEARN:
            self._on_learn(remote_id, inst, seq_id, val)
        elif paxos_type == PaxosType.NACK:
            self._on_nack(remote_id, inst, seq_id, val)

    def _on_prepare(self, remote_id, inst, seq_id):
        if not self.is_acceptor:
            self.log_error("not acceptor, ignore prepare from %s" % (remote_id))
            return
        data = self.instance_get_create(inst)
        if data.promised_seq > seq_id:
            self.send_msg(remote_id, PaxosType.NACK, inst, data.promised_seq, data.accepted_val)
            return
        data.promised_seq = seq_id
        if data.accepted_val is not None:
            self.send_msg(remote_id, PaxosType.PROMISE, inst, data.accepted_seq, data.accepted_val)
        else:
            self.send_msg(remote_id, PaxosType.PROMISE, inst, seq_id, None)
        return


    def _on_promise(self, remote_id, inst, seq_id, val):
        if not self.is_proposer:
            self.log_error("not proposer, ignore promise from %s" % (remote_id))
            return
        data = self.instance_get_create(inst)
        data.add_promise(remote_id, seq_id, val)
        if len(data.promised_quorum) > len(self.acceptor_ids) / 2.0 and data.propose_state == PaxosProposerState.PREPARE:
            # decide value and send propose 
            data.propose_state = PaxosProposerState.PROPOSE
            data.propose_last_ts = time.time()
            seq_id = data.propose_seq
            val = None
            if data.highest_accepted_val is not None:
                #TODO conflict detection, we now just believe everything decided will not change by replica
                val = data.accepted_val = data.highest_accepted_val
            else:
                val = data.propose_val
            for server_id in self.quorum_ids:
                self.send_msg(server_id, PaxosType.PROPOSE, inst, seq_id, val)


    def _on_propose(self, remote_id, inst, seq_id, val):
        if not self.is_acceptor:
            self.log_error("not acceptor, ignore propose from %s" % (remote_id))
            return
        data = self.instance_get_create(inst)
        if data.promised_seq > seq_id:
            self.send_msg(remote_id, PaxosType.NACK, inst, data.promised_seq, data.accepted_val)
            return
        data.promised_seq = seq_id
        data.accepted_seq = seq_id
        self.send_msg(remote_id, PaxosType.ACCEPTED, inst, seq_id, val)
        self.master_id = self.get_server_id_from_seq(seq_id)

    def accepted_callback(self, inst, val):
        raise NotImplementedError()

    def _notify_accepted(self, inst, val):
        try:
            self.accepted_callback(inst, val)
        except NotImplementedError:
            pass

    def _on_accepted(self, remote_id, inst, seq_id, val):
        """ assume that every paxos run will increase seq_id """
        if not self.is_proposer:
            return
        data = self.instance_get_create(inst)
        if data.propose_seq != seq_id:
            self.log_error("ignore wrong accepted seq_id %s from server %s, not expected %s" % (seq_id, remote_id, data.propose_seq))
            return
        data.accepted_quorum.add(remote_id)
        if len(data.accepted_quorum) <= len(self.acceptor_ids) / 2:
            return
        data.accepted_val = val
        data.propose_state = PaxosProposerState.ACCEPTED
        self.master_id = self.server_id
        self.is_master = True
        if data.start_ts:
            data.paxos_time = time.time() - data.start_ts
        self.log_info("i am now leader, broadcasting message")
        for server_id in self.all_peers:
            self.send_msg(server_id, PaxosType.LEARN, inst, seq_id, val)
        self._notify_accepted(inst, val)

    def _on_learn(self, remote_id, inst, seq_id, val):
        data = self.instance_get_create(inst)
        data.accepted_val = val
        data.propose_state = PaxosProposerState.ACCEPTED
        self.master_id = self.get_server_id_from_seq(seq_id)
        if self.master_id != self.server_id and self.is_master:
            self.is_master = False
        self.log_info("server %s is master, inst %s val %s seq %s" % (self.master_id, inst, seq_id, val))
        self._notify_accepted(inst, val)


    def _on_nack(self, remote_id, inst, seq_id, val):
        if not self.is_proposer:
            self.log_error("not proposer, ignore nack from %s" % (remote_id))
            return
        #TODO what to do with val
        data = self.instance_get_create(inst)
        if val is not None:
            data.accepted_val = val
            data.accepted_seq = seq_id
            data.paxos_time = time.time() - data.start_ts
            data.propose_state = PaxosProposerState.ACCEPTED
            self.master_id = self.get_server_id_from_seq(seq_id)
            self._notify_accepted(inst, val)
        if(val is None or data.retry) and seq_id > data.propose_seq: # old nack will ignore
            self.start_paxos(inst, knowned_seq=seq_id, retry=data.retry)


    def send_msg(self, remote_id, paxos_type, inst, seq_id, val=None):
        msg = PaxosMsg.make(self.server_id, paxos_type, inst, seq_id, val)
        self._on_msg_to_send(remote_id, msg)

    def start_paxos(self, inst, val=None, knowned_seq=0, retry=False):
        """ if retry is True, the proposer will always try to become leader """
        if not self.is_proposer:
            self.log_error("not proposer, cannot start paxos")
            return
        data = self.instance_get_create(inst)
        data.retry = retry
        data.propose_sent = False
        if val is not None:
            data.propose_val = val
        elif data.propose_val is None:
            raise Exception("must define a value to propose")
        data.increase_seq(self.server_id, self.server_numbers, knowned_seq)
        data.propose_state = PaxosProposerState.PREPARE
        data.start_ts = data.propose_last_ts = time.time()
        for server_id in self.quorum_ids:
            self.send_msg(server_id, PaxosType.PREPARE, inst, data.propose_seq, data.accepted_val)


def test():
    data = Instance(0, None)
    print data.increase_seq(0, 3), data.propose_seq
    print data.increase_seq(0, 3), data.propose_seq
    print data.increase_seq(9, 3), data.propose_seq
    print data.increase_seq(0, 3), data.propose_seq

if __name__ == '__main__':
    test()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :
