# -*- coding: utf-8 -*-

"""

This package implements the `ØMQ Majordomo Protocol` as specified under http://rfc.zeromq.org/spec:7

Terms used
----------

  worker
    process offering exactly one service in request/reply fashion.

  client
    independant process using a service in request/reply fashion.

  broker
    process routing messages from a client to a worker and back.

  worker id
    the  ØMQ socket identity of the worker socket communicating with the broker.

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

from client import MDPClient
from worker import MDPWorker
from broker import MDPBroker

### Local Variables:
### buffer-file-coding-system: utf-8
### mode: python
### End:
