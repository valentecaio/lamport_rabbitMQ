class Request:
    def __init__(self, timestamp, queue_name, access_duration=None):
        self.timestamp = timestamp
        self.owner_name = queue_name
        self.access_duration = access_duration

    def __repr__(self):
        return "(timestamp: %s, queue: %s, access_duration: %s)" % ( self.timestamp,self.owner_name, self.access_duration)

    # used by PriorityQueue for comparing elements
    def __lt__(self, other):
        return self.timestamp < other.timestamp
