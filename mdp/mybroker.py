# -*- coding: utf-8 -*-


import zmq
from zmq.eventloop.ioloop import IOLoop

from broker import MDPBroker

###

class MyBroker(MDPBroker):

    pass
#
###

if __name__ == '__main__':
    context = zmq.Context()
    broker = MyBroker(context, "tcp://127.0.0.1:5555")
    IOLoop.instance().start()
    broker.shutdown()


### Local Variables:
### buffer-file-coding-system: utf-8
### mode: python
### End:
