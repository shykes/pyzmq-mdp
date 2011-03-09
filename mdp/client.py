# -*- coding: utf-8 -*-

"""Module containing client functionality for the MDP implementation.

For the MDP specification see: http://rfc.zeromq.org/spec:7
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

from exceptions import UserWarning

import zmq
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop import IOLoop, DelayedCallback

###

class InvalidStateError(RuntimeError):
    """Exception raised when the requested action is not available due to socket state.
    """
    pass
#

class RequestTimeout(UserWarning):
    """Exception raised when the request timed out.
    """
    pass
#
###

class MDPClient(object):

    """Class for the MDP client side.

    Thin encapsulation of a zmq.REQ socket.
    Provides a request method with optional timout parameter.

    Will use a timeout to indicate a broker failure.
    """

    _proto_version = b'MDPC01'

    def __init__(self, context, endpoint, service):
        """Initialize the MDPClient.

        context is the zmq context to create the socket from.
        service is a byte-string with the service name for the requests.
        """
        socket = context.socket(zmq.REQ)
        ioloop = IOLoop.instance()
        self.service = service
        self.endpoint = endpoint
        self.stream = ZMQStream(socket, ioloop)
        self.stream.on_recv(self._on_message)
        self.can_send = True
        self._proto_prefix = [ self._proto_version, service]
        self._tmo = None
        self.timed_out = False
        socket.connect(endpoint)
        return

    def shutdown(self):
        """Method to deactivate the client connection completely.

        Will delete the stream and the underlying socket.
        """
        if not self.stream:
            return
        self.stream.socket.setsockopt(zmq.LINGER, 0)
        self.stream.socket.close()
        self.stream.close()
        self.stream = None
        return

    def request(self, msg, timeout=None):
        """Send the given message.

        msg can either be a byte-string or a list of byte-strings.
        timeout is the time to wait for response in milliseconds.
        """
        if not self.can_send:
            raise InvalidStateError()
        # prepare full message
        to_send = self._proto_prefix[:]
        if isinstance(msg, list):
            to_send.extend(msg)
        else:
            to_send.append(msg)
        self.stream.send_multipart(to_send)
        self.can_send = False
        if timeout:
            self._start_timeout(timeout)
        return

    def _on_timeout(self):
        """Helper called after timeout.
        """
        self.timed_out = True
        self._tmo = None
        self.on_timeout()
        return

    def _start_timeout(self, timeout):
        """Helper for starting the timeout.
        """
        self._tmo = DelayedCallback(self._on_timeout, timeout)
        self._tmo.start()
        return

    def _on_message(self, msg):
        """Helper method called on message receive.
        """
        if self._tmo:
            self._tmo.stop()
            self._tmo = None
        # setting state before invoking on_message, so we can request from there
        self.can_send = True
        self.on_message(msg)
        return

    def on_message(self, msg):
        """Public method called when a message arrived.

        Should be overloaded!
        """
        pass

    def on_timeout(self):
        """Public method called when a timeout occured.

        Should be overloaded!
        """
        pass
#

### Local Variables:
### buffer-file-coding-system: utf-8
### mode: python
### End:
