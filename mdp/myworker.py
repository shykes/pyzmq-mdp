# -*- coding: utf-8 -*-


import zmq
from zmq.eventloop.ioloop import IOLoop

from worker import MDPWorker

###

class MyWorker(MDPWorker):

    HB_INTERVAL = 2000
    HB_LIVENESS = 5

    count = 0

    def on_request(self, msg):
        self.count = self.count + 1
        answer = ['REPLY'] + msg
        self.reply(answer)
        if self.count > 2:
            print 'HALT!'
            IOLoop.instance().stop()
        return
#
###

if __name__ == '__main__':
    context = zmq.Context()
    worker = MyWorker(context, "tcp://127.0.0.1:5555", "echo")
    IOLoop.instance().start()
    worker.shutdown()


### Local Variables:
### buffer-file-coding-system: utf-8
### mode: python
### End: