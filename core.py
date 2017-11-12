class Request:
    def __init__(self, time, queue_name):
        self.time = time
        self.queue_name = queue_name

    def __repr__(self):
        return "queue_name: %s, time: %s" % (self.queue_name, self.time)
