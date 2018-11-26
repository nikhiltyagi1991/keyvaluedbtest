import threading
import random

str(index)+ "." + column_name, row[column_name]

class AccessLoadGenerator(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        columns = [
            'trip_id',
            'arrival_time',
            'departure_time',
            'stop_id',
            'stop_sequence',
            'stop_headsign',
            'pickup_type',
            'drop_off_type',
            'shape_dist_traveled'
        ]

        while(1):
            column_idx = random.randint(0, len(columns)-1)
            key = "stop_times." + str(random.randint(1,101)) + '.' + columns[column_idx]

if __name__ == '__main__':
    for x in range(100):
        print('Start Thread ', x+1)
        mythread = AccessLoadGenerator(name = str(x + 1))
        mythread.start()
