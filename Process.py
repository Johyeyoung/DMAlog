from collections import deque


class Process:
    def __init__(self, idx=None, _type=None):
        self.idx = idx
        self.type = _type  # 어떤 타입의 프로세스인지
        self.Q = deque()
        self.tps_t = 0  # TPS 몇초인지
        self.tps_start = None
        self.last_time = 0  # 가장 최근에 업데이트된 시간

