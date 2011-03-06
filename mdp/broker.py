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
        msg = [ self.id, b"MPDW01", chr(4) ]
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

    def __init__(self, context, master_ep, dtable, client_ep=None):
        socket = context.socket(zmq.XREP)
        socket.bind(master_ep)
        self.master_stream = ZMQStream(socket)
        self.dispatcher = Dispatcher(dtable)
        self.master_stream.on_recv(self.dispatcher.on_message)
        if client_ep:
            socket = context.socket(zmq.XREP)
            socket.bind(client_ep)
            self.client_stream = ZMQStream(socket)
            self.client_stream.on_recv(self.dispatcher.on_message)
        else:
            self.client_stream = self.master_stream
        self._workers = {}
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
            self._services[service].put(wid)
        else:
            q = ServiceQueue()
            q.put(wid)
            self._services[service] = q
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
            q = self._services[service]
            q.remove(wid)
        del self._workers[wid]
        return

    def shutdown(self):
        self.dispatcher = None
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

    def on_ready(self, ret_id, msg):
        """Process worker READY command.
        """
        print "broker received ready"
        print socketid2hex(ret_id)
        pprint(msg)
        print
        self.register_worker(ret_id, msg[0])
        return

    def on_reply(self, ret_id, msg):
        """Process worker REPLY command.
        """
        print "broker received REPLY"
        print socketid2hex(ret_id)
        pprint(msg)
        print
        wrep = self._workers[ret_id]
        service = wrep.service
        client, msg = split_address(msg)
        to_send = [ client, b'', service]
        to_send.extend(msg)
        self.client_stream.send_multipart(to_send)
        wq = self._services[service]
        wq.put(wrep.id)
        return

    def on_heartbeat(self, ret_id, msg):
        """Process worker HEARTBEAT command.
        """
##         print "broker received HB"
##         print socketid2hex(ret_id)
##         pprint(msg)
##         print
        try:
            worker = self._workers[ret_id]
            if worker.is_alive():
                worker.on_heartbeat()
        except KeyError:
            # ignore HB for unknown worker
            pass
        return

    def on_disconnect(self, ret_id, msg):
        """Process worker DISCONNECT command.
        """
        print "broker received DISCONNECT"
        print socketid2hex(ret_id)
        pprint(msg)
        print
        self.unregister_worker(ret_id)
        return

    def on_client(self, ret_id, msg):
        print "broker received CLIENT request"
        print socketid2hex(ret_id)
        pprint(msg)
        print
        service = msg.pop(0)
        try:
            wq = self._services[service]
            wid = wq.get()
            if not wid:
                # no worker ready
                # ignore message
                print 'broker has no worker for service "%s"' % service
                return
            wrep = self._workers[wid]
            to_send = [ wrep.id, b"MDPW01", b'\x02', ret_id, b'']
            to_send.extend(msg)
            self.master_stream.send_multipart(to_send)
        except KeyError:
            # unknwon service
            # ignore request
            print 'broker has no service "%s"' % service
        return

    def on_worker(self, ret_id, msg):
        cmd = msg.pop(0)
        if cmd in self._worker_cmds:
            fnc = self._worker_cmds[cmd]
            fnc(ret_id, msg)
        # ignore unknown command
        return
#

class Dispatcher(object):

    """Class responsible for dispatching incomming messages to the appropriate handler.

    Distinguishes between MDP clients and worker Messages.
    """

    def __init__(self, dtable):
        """Initialize instance.
        """
        self.table = dtable
        return

    def on_message(self, msg):
        """Processes given message.

        msg is a list of byte-strings representing the messages as received.
        """
        rp, msg = split_address(msg)
        # dispatch on first frame after path
        t = msg.pop(0)
        for pat, fnc in self.table.items():
            if t.startswith(pat):
                ret = fnc(rp, msg)
                return ret
        # ignores unknown messages
        return
#

class MDPBroker(BrokerBase):

    """Basic implementation of the MDP broker.
    """

    DISPATCH = {'MDPC':'on_client',
                'MDPW':'on_worker'}

    def __init__(self, context, master_ep, client_ep=None):
        dtable = dict((k, getattr(self, v)) for k,v in self.DISPATCH.items())
        super(MDPBroker, self).__init__(context, master_ep, dtable, client_ep)
        return

    def shutdown(self):
        super(MDPBroker, self).shutdown()
        return
#
###

### Local Variables:
### buffer-file-coding-system: utf-8
### mode: python
### End:
