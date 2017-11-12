class Request:
    def __init__(self, timestamp, queue_name, access_duration=None):
        self.timestamp = timestamp
        self.owner_name = queue_name
        self.access_duration = access_duration

    def __repr__(self):
        return "(queue_name: %s, timestamp: %s, access_duration: %s)" % (self.owner_name, self.timestamp, self.access_duration)
