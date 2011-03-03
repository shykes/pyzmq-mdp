# -*- coding: utf-8 -*-


import zmq
from zmq.eventloop.ioloop import IOLoop

from client import MDPClient

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
    client = MyClient(context, "tcp://127.0.0.1:5555", "echo")
    client.send(b'TEST', 2000)
    IOLoop.instance().start()
    client.shutdown()


### Local Variables:
### buffer-file-coding-system: utf-8
### mode: python
### End:
