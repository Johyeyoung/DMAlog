import itertools
import os
import re
import sys
from collections import deque, defaultdict
from heapq import heappush, heappop

from Process import *


class LogManager:
    def __init__(self):
        self.workspace1 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_seoul_20220803'
        self.workspace2 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_pusan_20220803'
        self.workspace3 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_pusan2_20220803'
        self.workspacetest = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_seoul_test'
        self.totalWorkspace = [self.workspacetest]
        #self.totalWorkspace = [self.workspace1, self.workspace2, self.workspace3]

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
        heap = []
        sub_line_list = set()
        time_key = defaultdict(int)
        for workspace in self.totalWorkspace:
            for file in os.listdir(workspace):
                a = re.compile(r'(lf_tr|lf_ts|ls_tr|ls_ts)').search(file)
                if a is not None:
                    filepath = os.path.join(workspace, file)
                    f = open(filepath, encoding='cp949')
                    sys.stdin = f
                    for line in f:
                        pass
                        try:
                            time = line.split('|')[2]
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
                        if sub_line[20:] not in sub_line_list:  # 중복제거를 위해
                            sub_line_list.add(sub_line[20:])
                            sub_result = self.doParse(sub_line, metaInfo, {'time': int(float(time))}, 20)
                            if sub_result['time'] in (90512, 90513, 90514, 90515, 90516, 90517):
                                heap.append(sub_result)
                                time_key[int(float(time))] += 1
        return heap, time_key


class Simulator:
    def __init__(self):
        self.log = defaultdict()  # 주문과 프로세스를 연결하는 곳
        self.processLst = list()

    def makeProcess(self, num):
        for idx in range(num):
            exec(f'process_{idx} = Process({idx})')
            exec(f'self.processLst.append(process_{idx})')


    def roundRobin(self, num, orderQueue):
        self.makeProcess(num)
        proc_num = 0  # 몇번 프로세스로 갈지
        TPS_log = defaultdict(list)
        total = list(orderQueue)[:]
        while orderQueue:
            order = orderQueue.popleft()
            # 집어넣을 프로세스 선택하기
            if order['TransCd'] == 'S':
                targetP = self.processLst[proc_num]
            else:
                orgOrder = order['OrgOrderID']
                org_proc_num = self.log[orgOrder]
                targetP = self.processLst[org_proc_num]

            self.log[order["OrderID"]] = int(targetP.name)  # 결과 기록

            # 기존 큐의 데이터는 처리되고도 남은 시간
            if targetP.last_time < order['time']:
                if targetP.tps_t != 0:  # Q에 남아있던 TPS 결과 처리도 여기서
                    TPS_log[targetP.name].append((targetP.Q[0]['time'], (targetP.Q[0]['time'] + targetP.tps_t)))
                    targetP.tps_t = 0
                targetP.Q = deque()

            targetP.Q.append(order)  # 새로운 데이터 추가
            targetP.last_time = targetP.last_time if targetP.last_time > order['time'] else order['time']

            if len(targetP.Q) >= 150:
                targetP.tps_t += 1
                targetP.Q = deque(list(targetP.Q)[150:])
                targetP.last_time += 1

            proc_num += 1
            if proc_num == num: proc_num = 0


        return TPS_log






if __name__ == "__main__":
    import timeit
    # start = timeit.default_timer()
    # stop = timeit.default_timer()
    # print(stop - start, sys.getsizeof(total))

    logManager = LogManager()
    total, time_key = logManager.run()

    sorted_time_key = sorted(time_key.items(), key=lambda item: item[1], reverse=True)
    print('전:', len(sorted_time_key))

    sorted_time_key = [x for x in sorted_time_key if x[1] > 150]
    print('후:', len(sorted_time_key))
    print(sorted(sorted_time_key, key=lambda x: x[0]))

    simulator = Simulator()
    TPS_log = simulator.roundRobin(1, deque(total))
    print(len(TPS_log[0]))
    print(TPS_log[0])

