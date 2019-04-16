import threading
import queue
import time


def job(d):
    print('task started ' + str(d))
    time.sleep(d)
    print('task finished ' + str(d))


class MyThread(threading.Thread):

    def __init__(self, q):
        threading.Thread.__init__(self)
        self.q = q

    def run(self):
        while True:
            if not self.q.empty():
                job(self.q.get())
                self.q.task_done()


if __name__ == '__main__':
    q = queue.Queue()
    start = time.time()
    for i in range(4):
        t = MyThread(q)
        t.setDaemon(True)
        t.start()

    for j in range(1, 5):
        q.put(j)

    q.join()
    print(time.time() - start)