# -*- coding: utf-8 -*-


"""Module containing the main components for the MDP broker.

For the MDP specification see: http://rfc.zeromq.org/spec:7

"""


__license__ = """
    This file is part of MDP.

    MDP is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    MDP is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with MDP.  If not, see <http://www.gnu.org/licenses/>.
"""

__author__ = 'Guido Goldstein'
__email__ = 'gst-py@a-nugget.de'
__version__ = '0.0'


import sys
import time
from exceptions import UserWarning
from pprint import pprint

import zmq
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop import IOLoop, DelayedCallback, PeriodicCallback

from util import socketid2hex, split_address

###

HB_INTERVAL = 1000  # in milliseconds
HB_LIVENESS = 5    # HBs to miss before connection counts as dead

###

class _WorkerRep(object):

    """Helper class to represent a worker in the broker.
    """

    def __init__(self, wid, service, stream):
        self.id = wid
        self.service = service
        self.curr_liveness = HB_LIVENESS
        self.stream = stream
        self.last_hb = 0
        self.hb_in_timer = PeriodicCallback(self.on_tick, HB_INTERVAL)
        self.hb_in_timer.start()
        self.hb_out_timer = PeriodicCallback(self.send_hb, HB_INTERVAL)
        self.hb_out_timer.start()
        return

    def on_tick(self):
        self.curr_liveness -= 1
        return

    def send_hb(self):
        msg = [ self.id, b'', b"MDPW01", chr(4) ]
        self.stream.send_multipart(msg)
        return

    def on_heartbeat(self, t=None):
        self.curr_liveness = HB_LIVENESS
        if t is None:
            t = time.time()
        self.last_hb = t
        return

    def is_alive(self):
        return self.curr_liveness > 0

    def shutdown(self):
        self.hb_in_timer.stop()
        self.hb_out_timer.stop()
        self.hb_in_timer = None
        self.hb_out_timer = None
        self.stream = None
        return
#

class ServiceQueue(object):

    """Class defining the Queue interface for workers for a service.
    """

    def __init__(self):
        self.q = []
        return

    def __contains__(self, wid):
        return wid in self.q

    def __len__(self):
        return len(self.q)

    def remove(self, wid):
        try:
            self.q.remove(wid)
        except ValueError:
            pass
        return

    def put(self, wid):
        if wid not in self.q:
            self.q.append(wid)
        return

    def get(self):
        if not self.q:
            return None
        return self.q.pop(0)
#

class BrokerBase(object):

    """The MDP broker base class.

    The broker routes messages from clients to appropriate workers based on the
    requested service.

    This base class defines the overall functionality and the API. Subclasses are
    ment to implement additional features (like logging).

    The broker uses ØMQ XREQ sockets to deal witch clients and workers. These sockets
    are wrapped in pyzmq streams to fit well into IOLoop.
    """

    CLIENT_PROTO = b'MDPC01'
    WORKER_PROTO = b'MDPW01'


    def __init__(self, context, master_ep, client_ep=None):
        socket = context.socket(zmq.XREP)
        socket.bind(master_ep)
        self.master_stream = ZMQStream(socket)
        self.master_stream.on_recv(self.on_message)
        if client_ep:
            socket = context.socket(zmq.XREP)
            socket.bind(client_ep)
            self.client_stream = ZMQStream(socket)
            self.client_stream.on_recv(self.on_message)
        else:
            self.client_stream = self.master_stream
        self._workers = {}
        # services contain the worker queue and the request queue
        self._services = {}
        self._worker_cmds = { '\x01': self.on_ready,
                              '\x03': self.on_reply,
                              '\x04': self.on_heartbeat,
                              '\x05': self.on_disconnect,
                              }
        self.hb_check_timer = PeriodicCallback(self.on_timer, HB_INTERVAL)
        self.hb_check_timer.start()
        return

    def register_worker(self, wid, service):
        """Register the worker id and add it to the given service.

        Does nothing if worker is already known.
        """
        if wid in self._workers:
            return
        self._workers[wid] = _WorkerRep(wid, service, self.master_stream)
        if service in self._services:
            wq, wr = self._services[service]
            wq.put(wid)
        else:
            q = ServiceQueue()
            q.put(wid)
            self._services[service] = (q, [])
        return

    def unregister_worker(self, wid):
        try:
            wrep = self._workers[wid]
        except KeyError:
            # not registered, ignore
            return
        wrep.shutdown()
        service = wrep.service
        if service in self._services:
            wq, wr = self._services[service]
            wq.remove(wid)
        del self._workers[wid]
        return

    def disconnect(self, wid):
        """Send disconnect command to given id and unregister worker.
        """
        to_send = [ wid, self.WORKER_PROTO, b'\x05' ]
        self.master_stream.send_multipart(to_send)
        self.unregister_worker(wid)
        return

    def shutdown(self):
        if self.client_stream == self.master_stream:
            self.client_stream = None
        self.master_stream.on_recv(None)
        self.master_stream.socket.setsockopt(zmq.LINGER, 0)
        self.master_stream.socket.close()
        self.master_stream.close()
        self.master_stream = None
        if self.client_stream:
            self.client_stream.on_recv(None)
            self.client_stream.socket.setsockopt(zmq.LINGER, 0)
            self.client_stream.socket.close()
            self.client_stream.close()
            self.client_stream = None
        self._workers = {}
        self._services = {}
        return

    def on_timer(self):
        """Method called on timer expiry.

        Determines which workers need a heartbeat.
        Send the HB to the workers.
        """
        for wrep in self._workers.values():
            if not wrep.is_alive():
                print "DEAD worker:", socketid2hex(wrep.id)
                self.unregister_worker(wrep.id)
        return

    def on_ready(self, rp, msg):
        """Process worker READY command.
        """
        ret_id = rp[0]
        self.register_worker(ret_id, msg[0])
        return

    def on_reply(self, rp, msg):
        """Process worker REPLY command.
        """
        ret_id = rp[0]
        wrep = self._workers[ret_id]
        service = wrep.service
        # build client reply and send it
        to_send, msg = split_address(msg)
        to_send.extend([b'', self.CLIENT_PROTO, service])
        to_send.extend(msg)
        self.client_stream.send_multipart(to_send)
        # make worker available again
        wq, wr = self._services[service]
        wq.put(wrep.id)
        if wr:
            proto, rp, msg = wr.pop(0)
            self.on_client(proto, rp, msg)
        return

    def on_heartbeat(self, rp, msg):
        """Process worker HEARTBEAT command.
        """
        ret_id = rp[0]
        try:
            worker = self._workers[ret_id]
            if worker.is_alive():
                worker.on_heartbeat()
        except KeyError:
            # ignore HB for unknown worker
            pass
        return

    def on_disconnect(self, rp, msg):
        """Process worker DISCONNECT command.
        """
        ret_id = rp[0]
        self.unregister_worker(ret_id)
        return

    def on_client(self, proto, rp, msg):
        """Method called on client message.

        proto is the protocol id sent.
        ret_id is the socket id where the message came from.

        Frame 0 is the requested service.
        The remaining frames are the request to forward to the worker.

        If the service is unknown to the broker or currently no worker available
        for the service, the message is ignored.
        """
        service = msg.pop(0)
        try:
            wq, wr = self._services[service]
            wid = wq.get()
            if not wid:
                # no worker ready
                # queue message
                msg.insert(0, service)
                wr.append((proto, rp, msg))
                return
            wrep = self._workers[wid]
            to_send = [ wrep.id, b'', self.WORKER_PROTO, b'\x02']
            to_send.extend(rp)
            to_send.append(b'')
            to_send.extend(msg)
            self.master_stream.send_multipart(to_send)
        except KeyError:
            # unknwon service
            # ignore request
            print 'broker has no service "%s"' % service
        return

    def on_worker(self, proto, rp, msg):
        """Method called on worker message.

        proto is the protocol id sent.
        ret_id is the socket id where the message came from.

        Frame 0 is the command id.
        The remaining frames depend on the command.

        This method determines the command sent by the worker and
        calls the appropriate method. If the command is unknown the
        message is ignored.
        """
        cmd = msg.pop(0)
        if cmd in self._worker_cmds:
            fnc = self._worker_cmds[cmd]
            fnc(rp, msg)
        # ignore unknown command
        return

    def on_message(self, msg):
        """Processes given message.

        msg is a list of byte-strings representing the messages as received.
        """
        rp, msg = split_address(msg)
        # dispatch on first frame after path
        t = msg.pop(0)
        if t.startswith(b'MDPW'):
            self.on_worker(t, rp, msg)
        elif t.startswith(b'MDPC'):
            self.on_client(t, rp, msg)
        else:
            print 'Broker unknown Protocol: "%s"' % t
        # ignores unknown messages
        return
#

class MDPBroker(BrokerBase):

    """Basic implementation of the MDP broker.
    """

    pass
#
###

### Local Variables:
### buffer-file-coding-system: utf-8
### mode: python
### End:
