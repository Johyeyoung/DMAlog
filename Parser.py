import itertools
import os
import re
import sys
from collections import deque, defaultdict
import operator

from Process import *
from tqdm import tqdm
import time
import mmap


class LogManager:
    def __init__(self):
        self.workspace1 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_seoul_20220803'
        self.workspace2 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_pusan_20220803'
        self.workspace3 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_pusan2_20220803'
        self.workspacetest = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_seoul_test'
        #self.totalWorkspace = [self.workspace1, self.workspace2, self.workspace3]
        self.totalWorkspace = [self.workspace1]  # 서울만 테스트

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
                                'AskTypeCd': 2,
                                'ProgramTT': 2,
                                'ClientArea': 20,
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
                         'ClientArea': 20}
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
                         'AskTypeCd': 2,
                         'ProgramTT': 2,
                         'ClientArea': 20,
                         'ProgramODT': 1,
                         'Filler': 11}
        # 체결 확정
        self.infoConfirm = {'OrderID': 10,
                            'OrgOrderID': 10,
                            'IssueCd': 12,}


    def doParse(self, line, metaInfoDict, result, start_idx=0):
        idx = start_idx
        for key, v_len in metaInfoDict.items():
            result[key] = line[idx: idx + int(v_len)]
            idx += int(v_len)
        return result

    def run(self):
        total = list()
        sub_line_set = set()
        time_key = defaultdict(int)
        orderKind_log = defaultdict()
        for workspace in tqdm(self.totalWorkspace, unit= "디렉토리 개수"):
            for file in tqdm(os.listdir(workspace), unit= "파일 개수", desc='파일 읽는 중'):
                a = re.compile(r'(lf_tr|lf_ts|ls_tr|ls_ts)').search(file)
                if a is not None:
                    filepath = os.path.join(workspace, file)
                    with open(filepath, 'r+b') as f:
                        map_file = mmap.mmap(f.fileno(), 0)
                        for line in iter(map_file.readline, b""):
                            line = line.decode('euc-kr')
                            try:
                                time = line.split('|')[2]
                                sub_line = line.split(']')[1].split('[')[1]
                                TransCd = sub_line[20]
                                if (sub_line[0:4] == '0121' and len(sub_line) == 141) or\
                                    (sub_line[0:4] == '0109' and len(sub_line) == 129):
                                    pass
                                else:
                                    continue
                            except:
                                continue
                            metaInfo = self.infoOrderSingle if TransCd == 'S' else self.infoFill if TransCd == 'F' else self.infoEdit
                            if sub_line[20:] in sub_line_set:
                                continue  # 중복제거
                            sub_line_set.add(sub_line[20:])
                            sub_result = self.doParse(sub_line, metaInfo,  {'RealTime': time, 'time': int(float(time))}, 20)
                           # if sub_result['IssueCd'][:3] != 'KR4' or sub_result['TransCd'] != 'S': continue  # 잠시 파생만 저장
                            if sub_result['IssueCd'][:3] == 'KR4' or sub_result['TransCd'] != 'S': continue  # 잠시 현물만 저장

                            orderKind_log[sub_result['OrderID']] = sub_result['OrderKindCd']

                            total.append(sub_result)
                            time_key[int(float(time))] += 1
                        map_file.close()
        print('데이터를 정렬합니다.')
        total.sort(key=operator.itemgetter('RealTime'))
        print('정렬이 완료되었습니다.')
        return deque(total), time_key, orderKind_log

    def run_ans(self, _re=None):
        total = list()
        sub_line_set = set()
        time_key = defaultdict(int)
        proc_log = defaultdict()  # 프로세스의 로그를 기록하는 곳
        for workspace in tqdm(self.totalWorkspace, unit="디렉토리 개수"):
            for file in tqdm(os.listdir(workspace), unit="파일 개수", desc='파일 읽는 중'):
                if _re is None: _re = r'(lf_xs)'
                a = re.compile(_re).search(file)
                if a is not None:
                    filepath = os.path.join(workspace, file)
                    with open(filepath, 'r+b') as f:
                        map_file = mmap.mmap(f.fileno(), 0)
                        for line in iter(map_file.readline, b""):
                            line = line.decode('cp949')
                            try:
                                time = line.split('|')[2]
                                sub_line = line.split(']')[1].split('[')[1].split('                    ')[1]
                                sub_result = self.doParse(sub_line, self.infoConfirm, {'RealTime': time, 'time': int(float(time))}, 55)
                            except:
                                continue
                            if (sub_result['OrderID'] in sub_line_set) or (sub_line[20] == 'N'):
                                continue
                            #if sub_result['IssueCd'][:3] != 'KR4': continue  # 잠시 파생만 저장
                            if sub_result['IssueCd'][:3] == 'KR4' or sub_result['TransCd'] != 'S': continue  # 잠시 현물만 저장

                            sub_line_set.add(sub_result['OrderID'])
                            sub_result['ProcNum'] = int(filepath[-1])-1  # 몇번 프로세스로 갔는지 정보
                            proc_log[sub_result['OrderID']] = sub_result['ProcNum']

                            total.append(sub_result)
                            time_key[int(float(time))] += 1
                        map_file.close()
        print('데이터를 정렬합니다.')
        total.sort(key=operator.itemgetter('RealTime'))
        print('정렬이 완료되었습니다.')

        return deque(total), time_key, proc_log




if __name__ == "__main__":
    import timeit

    logManager = LogManager()

    # start = timeit.default_timer()
    # total, time_key = logManager.run()
    # stop = timeit.default_timer()
    # print(stop - start)  # 42.19112780003343

    total, time_key, proc_log = logManager.run_ans()
    sorted_time_key = sorted(time_key.items(), key=lambda item: item[1], reverse=True)
    sorted_time_key = [x for x in sorted_time_key if x[1] >= 150]
   # print(sorted(sorted_time_key, key=lambda x: x[1], reverse=True))
   # print(sorted(sorted_time_key, key=lambda x: x[0], reverse=True))

    total, time_key, orderKind_log = logManager.run()
    sorted_time_key = sorted(time_key.items(), key=lambda item: item[1], reverse=True)
    sorted_time_key = [x for x in sorted_time_key if x[1] >= 150]
    # print(sorted(sorted_time_key, key=lambda x: x[1], reverse=True))
    # print(sorted(sorted_time_key, key=lambda x: x[0], reverse=True))
