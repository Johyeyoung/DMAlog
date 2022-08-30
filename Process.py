from collections import deque

class Process:
    def __init__(self, name):
        self.name = name
        self.logData = list()
        self.Q = deque()
        self.tps_t = 0  # TPS 몇초인지
        self.last_time = 0
        self.tps_start = None

