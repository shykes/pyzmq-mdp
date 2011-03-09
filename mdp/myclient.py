# -*- coding: utf-8 -*-

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

import zmq
from zmq.eventloop.ioloop import IOLoop

from client import MDPClient, mdp_request

###

class MyClient(MDPClient):

    def on_message(self, msg):
        print "Received:", repr(msg)
        IOLoop.instance().stop()
        return

    def on_timeout(self):
        print 'TIMEOUT!'
        IOLoop.instance().stop()
        return
#
###

if __name__ == '__main__':
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect("tcp://127.0.0.1:5555")
    res = mdp_request(socket, b'echo', [b'TEST'], 2.0)
    if res:
        print "Reply:", repr(res)
    else:
        print 'Timeout!'
    socket.close()
#

### Local Variables:
### buffer-file-coding-system: utf-8
### mode: python
### End:
