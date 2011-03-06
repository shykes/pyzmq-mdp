# -*- coding: utf-8 -*-


import zmq
from zmq.eventloop.ioloop import IOLoop

from worker import MDPWorker

###

class MyWorker(MDPWorker):

    HB_INTERVAL = 1000
    HB_LIVENESS = 3

    count = 0

    def on_request(self, msg):
        self.count = self.count + 1
        answer = ['REPLY'] + msg
        self.reply(answer)
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
