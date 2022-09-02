import os
from collections import deque, defaultdict
import operator
from MarketDataReader import *
from Parser import *

from Process import Process

class Simulator:
    def __init__(self, type, total_proc_num):
        self.proc_log = defaultdict()  # 주문과 프로세스를 연결하는 곳
        self.processList, self.proc_num = list(), 0
        self.createProcess(type, total_proc_num) # 할당된 프로세스를 생성
        self.compLog = defaultdict(list)

    def createProcess(self, type, total_proc_num):
        for idx in range(total_proc_num):
            exec(f'process_{idx} = Process({idx}, \'{type}\')')
            exec(f'self.processList.append(process_{idx})')
        print(f'{total_proc_num}개의 {type.upper()} Porcess가 생성되었습니다.')

    def selectProcess(self, order):
        try:
            orgOrder = order['OrgOrderID']
            org_proc_num = self.proc_log[orgOrder][1]
            targetP = self.processList[org_proc_num]
            self.proc_log[order["OrderID"]] = [order["RealTime"], int(targetP.idx),
                                               (orgOrder, org_proc_num)]  # process idx로 뽑아올 수 있게 인덱스로 저장
        except:
            self.proc_num += 1
            if self.proc_num == len(self.processList):
                self.proc_num = 0
            targetP = self.processList[self.proc_num]
            self.proc_log[order["OrderID"]] = [order["RealTime"], int(targetP.idx)]  # process idx로 뽑아올 수 있게 인덱스로 저장

        return targetP

    def roundRobin(self, order):
            # ... select process
            targetP = self.selectProcess(order)

            if order["time"] not in self.compLog.keys():
                self.compLog[order["time"]] = [0]*10
            self.compLog[order["time"]][targetP.idx] += 1

            #  Q안에 있는 order보다 뒤면 이미 다 처리됨 비우고 시작
            if targetP.last_time < order['time']:
                if targetP.tps_t != 0:
                    print(f"process_{targetP.type}_{targetP.idx} ▷ TPS가 종료되었습니다. ▶▶ "
                          f"현재시각 {targetP.last_time}: (TPS 구간) {targetP.tps_start} ~ {targetP.tps_start + targetP.tps_t - 1}")
                    targetP.tps_t, targetP.tps_start = 0, None
                targetP.Q.clear()

            # ... add order
            targetP.Q.append(order)
            targetP.last_time = targetP.last_time if targetP.last_time > order['time'] else order['time']

            # ... Q가 150개로 차면 비워주기
            if len(targetP.Q) >= 150:
                if targetP.tps_t == 0: targetP.tps_start = order['time']
                comment = '시작되었습니다.' if targetP.tps_t == 0 else '    진행중   '
                print(f"process_{targetP.type}_{targetP.idx} ▶ TPS가 {comment} ▶▶ "
                      f"현재시각 {targetP.last_time}: 주문 처리({len(targetP.Q)}건) {targetP.Q[0]['time']}~{targetP.Q[-1]['time']}")
                targetP.tps_t += 1
                targetP.Q = deque(list(targetP.Q)[150:])
                targetP.last_time += 1  # 소요된 1초의 시간 반영

            # ... 끝나고 나서 기록
            if len(orderQueue) == 0 and targetP.tps_t != 0:
                print(f"process_{targetP.type}_{targetP.idx} ▷ TPS가 종료되었습니다. ▶▶ "
                      f"현재시각 {targetP.last_time}: (TPS 구간) {targetP.tps_start} ~ {targetP.tps_start + targetP.tps_t - 1}")

    def clearProcess(self):
        # ... 끝나고 나서 기록
        for targetP in self.processList:
            if targetP.tps_t != 0:
                print(f"process_{targetP.type}_{targetP.idx} ▷ TPS가 종료되었습니다. ▶▶ "
                      f"현재시각 {targetP.last_time}: (TPS 구간) {targetP.tps_start} ~ {targetP.tps_start + targetP.tps_t - 1}")


class OrderClassifier:
    def __init__(self):
        main_path = 'C:/Users/USER/Downloads/mm/자료/시세, 배치정보'
        feed_spec = os.path.join(main_path, "feed spec")
        feed_file = os.path.join(main_path, "feed file/20220803.batch.log")
        self.equityLoader = EquityLoader(batchpath=feed_file, specpath=feed_spec)

    def classifyOrder(self, order):
        issueCd = order['IssueCd']
        if issueCd[:3] == 'KR4':
            return 'future'
        else:
            marketType = self.equityLoader.getStockInfo(issueCd).marketType
            return 'kosdaq' if marketType == 2 else 'kospi'





if __name__ == "__main__":

    logManager = LogManager()

    # 시뮬레이션
    orderQueue, time_key, orderKind_log = logManager.run()
    #orderQueue, time_key, proc_log = logManager.run_ans()

    # simulation 리스트 생성
    proc_info = {'kospi': 4, 'kosdaq': 2, 'future': 3}
    simulationList = dict()
    for type, proc_num in proc_info.items():
        simulationList[type] = Simulator(type, proc_num)

    # type 분류기
    classifier = OrderClassifier()
    while orderQueue:
        order = orderQueue.popleft()
        orderType = classifier.classifyOrder(order)

        # 종목 타입에 맞는 시뮬레이션을 꺼내서
        _simulator = simulationList[orderType]
        _simulator.roundRobin(order)

    # 다 끝나고 프로세스 기록 비우기
    for _simulator in simulationList.values():
        _simulator.clearProcess()



    # 실제 프로세스 결과
    comp_proc_log = defaultdict(list)
    comp_Queue_log = list()
    type = 'kospi'
    future = r'(lf_xs_fm00)'
    equity = r'(ls_xs_fm00)'
    orderQueue_ans, time_key_ans, proc_log = logManager.run_ans(future)

    for order in orderQueue_ans:
        if order['time'] not in comp_proc_log.keys():
            comp_proc_log[order['time']] = [0]*10
        comp_proc_log[order['time']][order['ProcNum']] += 1

    # 실제용
    with open("real_E.txt", 'w', encoding='cp949') as f:
        for key, value in comp_proc_log.items():
            f.write('%s:%s %s\n' % (key, value, sum(value)))

    orderQueue_ans = list(orderQueue_ans)
    orderQueue_ans.sort(key=operator.itemgetter('RealTime'))
    # 실제용 세부기록
    with open("real_E_info.txt", 'w', encoding='cp949') as f:
        for order in orderQueue_ans:
            if order['OrgOrderID'] != '          ':
                f.write('%s type(%s):[%s, %s, (%s, %s)]\n' % (order['OrderID'], orderKind_log[order['OrderID']], order['time'], order['ProcNum'], order['OrgOrderID'], proc_log[order['OrgOrderID']]))
            else:
                f.write('%s type(%s):[%s, %s]\n' % (order['OrderID'], orderKind_log[order['OrderID']], order['time'], order['ProcNum'],))


   # 시뮬레이션용
    with open("simulaton_E.txt", 'w', encoding='cp949') as f:
        for key, value in simulationList[type].compLog.items():
            f.write('%s:%s %s\n' % (key, value, sum(value)))

    # 시뮬레이션용 세부기록
    sorted_dict = sorted(simulationList[type].proc_log.items(), key=lambda item: item[1][0])
    with open("simulaton_E_info.txt", 'w', encoding='cp949') as f:
        for orderId, value in sorted_dict:
            value[0] = int(float(value[0]))
            f.write('%s type(%s):%s\n' % (orderId, orderKind_log[orderId], value))
