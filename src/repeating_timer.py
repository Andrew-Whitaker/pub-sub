import threading
import time

class RepeatingTimer(threading.Thread):
    def __init__(self, event):
        threading.Thread.__init__(self)
        self.stopped = event

    def start(self, duration, callback):
        t = threading.Thread(target=self.run, args=(duration, callback))
        t.start()
        return t

    def run(self, wait_duration, callback):
        while not self.stopped.wait(wait_duration):
            callback()

    def stop(self):
        self.stopped.set()

if __name__ == "__main__":
    e = threading.Event()
    r = RepeatingTimer(e)
    r.start().join()
    time.sleep(6)
    r.stop()
