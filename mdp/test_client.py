# -*- coding: utf-8 -*-

"""Unittests for the MDPClient class.
"""


__license__ = """
    This file is part of MDP.

    MDP is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    MDP is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with MDP.  If not, see <http://www.gnu.org/licenses/>.
"""
__author__ = 'Guido Goldstein'
__email__ = 'gst-py@a-nugget.de'

import sys
import time
import unittest
from pprint import pprint

import zmq
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop import IOLoop, DelayedCallback

from client import MDPClient, InvalidStateError

###

_do_print = False

###

class MyClient(MDPClient):

    def on_message(self, msg):
        if _do_print:
            print 'client received:',
            pprint(msg)
        self.last_msg = msg
        IOLoop.instance().stop()
        return

    def on_timeout(self):
        if _do_print:
            print 'client timed out!'
        IOLoop.instance().stop()
        return
#
###

class Test_MDPClient(unittest.TestCase):

    endpoint = b'tcp://127.0.0.1:7777'
    service = b'test'


    def setUp(self):
        self.context = zmq.Context()
        self.broker = None
        self._msgs = []
        return

    def tearDown(self):
        if self.broker:
            self._stop_broker()
        self.broker = None
        self._msgs = []
        self.context.term()
        self.context = None
        return

    def _on_msg(self, msg):
        self._msgs.append(msg)
        if _do_print:
            print 'broker received:',
            pprint(msg)
        if self.broker.do_reply:
            new_msg = msg[:4]
            new_msg.append(b'REPLY')
            self.broker.send_multipart(new_msg)
        else:
            IOLoop.instance().stop()
        return

    def _start_broker(self, do_reply=False):
        """Helper activating a fake broker in the ioloop.
        """
        socket = self.context.socket(zmq.XREP)
        self.broker = ZMQStream(socket)
        self.broker.socket.setsockopt(zmq.LINGER, 0)
        self.broker.bind(self.endpoint)
        self.broker.on_recv(self._on_msg)
        self.broker.do_reply = do_reply
        return

    def _stop_broker(self):
        if self.broker:
            self.broker.socket.close()
            self.broker.close()
            self.broker = None
        return

    # tests follow

    def test_01_create_01(self):
        """Test MDPclient simple create.
        """
        client = MDPClient(self.context, self.endpoint, self.service)
        self.assertEquals(self.endpoint, client.endpoint)
        self.assertEquals(self.service, client.service)
        client.shutdown()
        return

    def test_02_send_01(self):
        """Test MDPclient simple request.
        """
        self._start_broker()
        client = MDPClient(self.context, self.endpoint, self.service)
        client.request(b'XXX')
        IOLoop.instance().start()
        client.shutdown()
        self.assertEquals(len(self._msgs), 1)
        rmsg = self._msgs[0]
        # msg[0] is identity of sender
        self.assertEquals(rmsg[1], b'') # routing delimiter
        self.assertEquals(rmsg[2], client._proto_version)
        self.assertEquals(rmsg[3], self.service)
        self.assertEquals(rmsg[4], b'XXX')
        self._stop_broker()
        return

    def test_02_send_02(self):
        """Test MDPclient multipart request.
        """
        mydata = [b'AAA', b'bbb']
        self._start_broker()
        client = MDPClient(self.context, self.endpoint, self.service)
        client.request(mydata)
        IOLoop.instance().start()
        client.shutdown()
        self.assertEquals(len(self._msgs), 1)
        rmsg = self._msgs[0]
        # msg[0] is identity of sender
        self.assertEquals(rmsg[1], b'') # routing delimiter
        self.assertEquals(rmsg[2], client._proto_version)
        self.assertEquals(rmsg[3], self.service)
        self.assertEquals(rmsg[4:], mydata)
        self._stop_broker()
        return

    def test_02_send_03(self):
        """Test MDPclient request in invalid state.
        """
        client = MDPClient(self.context, self.endpoint, self.service)
        client.request(b'XXX') # ok
        self.assertRaises(InvalidStateError, client.request, b'AAA')
        client.shutdown()
        return

    def test_03_timeout_01(self):
        """Test MDPclient request w/ timeout.
        """
        client = MyClient(self.context, self.endpoint, self.service)
        client.request(b'XXX', 20) # 20 millisecs timeout
        IOLoop.instance().start()
        client.shutdown()
        self.assertEquals(client.timed_out, True)
        return

    def test_04_receive_01(self):
        """Test MDPclient message receive.
        """
        self._start_broker(do_reply=True)
        client = MyClient(self.context, self.endpoint, self.service)
        client.request(b'XXX')
        IOLoop.instance().start()
        client.shutdown()
        self._stop_broker()
        self.assertEquals(True, hasattr(client, b'last_msg'))
        self.assertEquals(3, len(client.last_msg))
        self.assertEquals(b'REPLY', client.last_msg[-1])
        self.assertEquals(self.service, client.last_msg[-2])
        return
#
###

if __name__ == '__main__':
    sys.argv.append('-v')
    unittest.main()
#

### Local Variables:
### buffer-file-coding-system: utf-8
### mode: python
### End:
