# -*- coding: utf-8 -*-


# -*- coding: utf-8 -*-


"""Module containing the main components for the MDP broker.

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


###

def socketid2hex(sid):
    """Returns printable hex representation of a socket id.
    """
    ret = ''.join("%02X" % ord(c) for c in sid)
    return ret
#

def split_address(msg):
    """Function to split return Id and message received by XREP.

    Returns 2-tuple with return Id and remaining message parts.
    Empty frames after the Id are stripped.
    """
    ret_ids = []
    for i, p in enumerate(msg):
        if p:
            ret_ids.append(p)
        else:
            break
    return (ret_ids, msg[i+1:])
#
###

### Local Variables:
### buffer-file-coding-system: utf-8
### mode: python
### End:
