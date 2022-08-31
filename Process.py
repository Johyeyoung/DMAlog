from collections import deque
from MarketDataReader import *

class Process:
    def __init__(self, idx=None, _type=None):
        self.idx = idx
        self.type = _type  # 어떤 타입의 프로세스인지
        self.Q = deque()
        self.tps_t = 0  # TPS 몇초인지
        self.tps_start = None
        self.last_time = 0  # 가장 최근에 업데이트된 시간



class ProcessManager:
    def __init__(self, proc_info):
        self.proc_info = proc_info  # {'kospi': 3, 'kosdaq': 3, 'future': 3}
        self.future_processLst, self.future_proc_num = list(), -1
        self.kospi_processLst, self.kospi_proc_num = list(), -1
        self.kosdaq_processLst, self.kosdaq_proc_num = list(), -1
        self.createProcess()

    def createProcess(self):
        # ... create process
        for type, num in self.proc_info.items():
            for idx in range(num):
                exec(f'process_{idx} = Process({idx}, \'{type}\')')
                exec(f'self.{type}_processLst.append(process_{idx})')
            print(f'{num}개의 {type} Process가 생성되었습니다.')
