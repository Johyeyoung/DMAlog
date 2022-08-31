import itertools
import os
import re
import sys
from collections import deque, defaultdict
import operator

from Process import *
from tqdm import tqdm
import time

class LogManager:
    def __init__(self):
        self.workspace1 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_seoul_20220803'
        self.workspace2 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_pusan_20220803'
        self.workspace3 = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_pusan2_20220803'
        self.workspacetest = 'C:/Users/USER/Downloads/mm/자료/DMALog/log file/feplog_seoul_test'
        #self.totalWorkspace = [self.workspacetest]
        self.totalWorkspace = [self.workspace1, self.workspace2, self.workspace3]

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

    def doParse(self, line, metaInfoDict, result, start_idx=0):
        idx = start_idx
        for key, v_len in metaInfoDict.items():
            result[key] = line[idx: idx + int(v_len)]
            idx += int(v_len)
        return result

    def run(self):
        total = []
        sub_line_list = set()
        time_key = defaultdict(int)
        for workspace in tqdm(self.totalWorkspace, unit= "디렉토리 개수"):
            for file in tqdm(os.listdir(workspace), unit= "파일 개수", desc='파일 읽는 중'):
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
                            #if sub_result['time'] in (90512, 90513, 90514, 90515, 90516, 90517):
                            total.append(sub_result)
                            time_key[int(float(time))] += 1
        print('데이터를 정렬합니다.')
        total.sort(key=operator.itemgetter('time'))
        print('데이터 정렬이 완료되었습니다.')
        return total, time_key

if __name__ == "__main__":

    logManager = LogManager()
    total, time_key = logManager.run()
