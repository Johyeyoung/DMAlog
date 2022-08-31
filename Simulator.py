import os
from collections import deque, defaultdict
import operator

from Process import Process
from Parser import *
from MarketDataReader import *


class Simulator:
    def __init__(self, proc_info):
        self.log = defaultdict()  # 주문과 프로세스를 연결하는 곳
        self.equityLoader = None
        self.procMngr = ProcessManager(proc_info)

    def getStockType(self, issueCd):
        if self.equityLoader is None:
            print('... 현물 종목 정보를 가져오고 있습니다.')
            main_path = 'C:/Users/USER/Downloads/mm/자료/시세, 배치정보'
            feed_spec = os.path.join(main_path, "feed spec")
            feed_file = os.path.join(main_path, "feed file/20220803.batch.log")
            self.equityLoader = EquityLoader(batchpath=feed_file, specpath=feed_spec)

        if issueCd[:3] == 'KR4':
            return 'future'
        else:
            marketType = self.equityLoader.getStockInfo(issueCd).marketType
            return 'kosdaq' if marketType == 2 else 'kospi'


    def selectProcess(self, order):
        type = self.getStockType(order['IssueCd'])
        target_processLst = eval(f'self.procMngr.{type}_processLst')
        try:
            orgOrder = order['OrgOrderID']
            org_proc_num = self.log[orgOrder]
            targetP = target_processLst[org_proc_num]
        except:
            proc_num = eval(f'self.procMngr.{type}_proc_num')
            proc_num = 0 if (proc_num + 1) == self.procMngr.proc_info[type] else proc_num + 1
            exec(f'self.procMngr.{type}_proc_num = proc_num')
            targetP = target_processLst[proc_num]

        # ... save log of selected process
        self.log[order["OrderID"]] = int(targetP.idx)  # process idx로 뽑아올 수 있게 인덱스로 저장
        return targetP


    def roundRobin(self, orderQueue):
        while orderQueue:
            order = orderQueue.popleft()

            # ... select process
            targetP = self.selectProcess(order)

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




if __name__ == "__main__":

    logManager = LogManager()
    total, time_key = logManager.run()

    sorted_time_key = sorted(time_key.items(), key=lambda item: item[1], reverse=True)
    sorted_time_key = [x for x in sorted_time_key if x[1] > 300]
    print(sorted(sorted_time_key, key=lambda x: x[0]))

    proc_info = {'kospi': 3, 'kosdaq': 3, 'future': 3}
    simulator = Simulator(proc_info)
    simulator.roundRobin(deque(total))

