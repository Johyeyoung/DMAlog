import Process
from Parser import *
from Simulator import *

if __name__ == "__main__":

    logManager = LogManager()
    total, time_key = logManager.run()

    sorted_time_key = sorted(time_key.items(), key=lambda item: item[1], reverse=True)
    sorted_time_key = [x for x in sorted_time_key if x[1] > 300]
    print(sorted(sorted_time_key, key=lambda x: x[0]))

    proc_info = {'kospi': 3, 'kosdaq': 3, 'future': 3}
    simulator = Simulator(proc_info)
    TPS_log = simulator.roundRobin(deque(total))

