import time


class Lock(object):
    def __init__(self, opq, name, cb):
        self.opq = opq
        self.name = name
        self.cb = cb
        self.lock = None

    def __enter__(self):
        self.lock = self.cb.volumeLock(self.opq, self.name)

    def __exit__(self, type, value, traceback):
        self.cb.volumeUnlock(self.opq, self.lock)
        # Context manager should return False unless it's handled exceptions
        return False


class PollLock(Lock):

    def __init__(self, opq, name, cb, poll_period):
        super(PollLock, self).__init__(opq, name, cb)
        self.poll_period = poll_period

    def __enter__(self):
        while True:
            self.lock = self.cb.volumeTryLock(self.opq, self.name)
            if self.lock:
                return
            time.sleep(self.poll_period)
