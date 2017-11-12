class Request:
    def __init__(self, time, queue_name):
        self.time = time
        self.owner_name = queue_name

    def __repr__(self):
        return "queue_name: %s, time: %s" % (self.owner_name, self.time)
