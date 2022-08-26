import os
import re
import sys
from collections import deque





class LogManager:
    def __init__(self):
        self.workspace1 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_seoul_20220803'
        self.workspace2 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_pusan_20220803'
        self.workspace3 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_pusan2_20220803'

        # 주문 정보 4.1
        self.infoOrderSingle = {'TransCd': 1,
                                'BoardID': 2,
                                'OrderID': 10,
                                'OrgOrderID': 10,
                                'IssueCd': 12,
                                'Ask/Bid': 1,
                                'OrderKindCd': 1,
                                'AccountNum': 12,
                                'OrderQuantity': 10,
                                'OrderPrice': 11,
                                'OrderType': 1,
                                'OrderCondition': 1,
                                'IP': 12,
                                'ClientArea': 20,
                                'AskTypeCd': 2,
                                'ProgramTT': 2,
                                'ProgramODT': 1, }
        # 체결 정보 4.2
        self.infoFill = {'TransCd': 1,
                         'OrderID': 10,
                         'IssueCd': 12,
                         'AccountNum': 12,
                         'KRXExctNum': 11,
                         'KRXExctPrice': 11,
                         'KRXExctQuantity': 10,
                         'SessionID': 2,
                         'KRXExctTime': 9,
                         'TNMTPrice': 11,
                         'TFMTPrice': 11,
                         'Ask/Bid': 1,
                         'ClientArea': 20,}
        # 정정 매매 4.3
        self.infoEdit = {'TransCd': 1,
                         'OrderID': 10,
                         'OrgOrderID': 10,
                         'IssueCd': 12,
                         'Ask/Bid': 1,
                         'OrderKindCd': 1,
                         'AccountNum': 12,
                         'OrderQuantity': 10,
                         'OrderPrice': 11,
                         'OrderType': 1,
                         'OrderCondition': 1,
                         'A/COrderQuantity': 10,
                         'AutoCancleType': 1,
                         'OrderRejectCd': 4,
                         'ClientArea': 20,
                         'AskTypeCd': 2,
                         'ProgramTT': 2,
                         'ProgramODT': 1,
                         'Filler': 11}

    def doParse(self, line, metaInfoDict, result, start_idx=0):
        idx = start_idx
        for key, v_len in metaInfoDict.items():
            result[key] = line[idx: idx + int(v_len)]
            idx += int(v_len)
        return result

    def run(self):
        for workspace in [self.workspace1, self.workspace2, self.workspace3]:
            for file in os.listdir(workspace):
                a = re.compile(r'(lf_tr|lf_ts|ls_tr|ls_ts)').search(file)
                if a is not None:
                    filepath = os.path.join(workspace, file)
                    f = open(filepath, encoding='cp949')
                    sys.stdin = f
                    for line in f:
                        time = line.split('|')[2]
                        try:
                            sub_line = line.split(']')[1].split('[')[1]
                            TransCd = sub_line[20]
                            if sub_line[0:4] == '0121' and len(sub_line) == 141:
                                pass
                            elif sub_line[0:4] == '0109' and len(sub_line) == 129:
                                pass
                            else:
                                continue
                        except:
                            continue
                        metaInfo = self.infoOrderSingle if TransCd == 'S' else self.infoFill if TransCd == 'F' else self.infoEdit
                        sub_result = self.doParse(sub_line, metaInfo, {'time': time}, 20)
                        print(sub_result)
                        #yield sub_result


if __name__ == "__main__":
    logManager = LogManager()
    import timeit

    start = timeit.default_timer()
    total = logManager.run()  #yield
    stop = timeit.default_timer()
    print(stop - start, sys.getsizeof(total))
    for x in list(total):
        print(x)



    # 어떤 프로세스가 TPS가 발생했는지 몇초부터 몇초까지 최대 얼마까지 찍었는지도 보여주기

class TPSFinder:
    def __init__(self):
        pass

class Processor:
    inputCnt = 0
    logData = list()

class Simulation:
    def __init__(self):
        self.inputRoundRobin()
        self.pocessor0 = Processor()
        self.pocessor1 = Processor()
        self.pocessor2 = Processor()
        self.Q = deque()


    def inputRoundRobin(self, queue):
        proc = 0  # 몇번 프로세스로 갈지
        while True:
            order = queue.popleft()
            exec(f'self.pocessor{proc}.inputCnt += 1')
            if order['TransCd'] != 'S':
                proc += 1

            else:
                orgOrder = order['OrgOrderID']
                proc -=

            if proc == 3: proc = 0

    def processLoger(self, order):

        order['OrderID']








