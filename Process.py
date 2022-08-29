from collections import deque

class Process:
    def __init__(self, name):
        self.name = name
        self.logData = list()
        self.Q = deque()
        self.tps_t = 0  # TPS 몇초인지
        self.last_time = 0

# 어떤 프로세스가 TPS가 발생했는지 몇초부터 몇초까지 최대 얼마까지 찍었는지도 보여주기
class TPSFinder:
    def __init__(self, process):
        pass

    def returnTime(self):
        pass


    def cutSlice(self):
        import time
        time.sleep(1)