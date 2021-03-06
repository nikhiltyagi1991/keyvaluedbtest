import threading
import random
import memcache, redis
import time, csv

# str(index)+ "." + column_name, row[column_name]
# mc = memcache.Client(['127.0.0.1:11211'], debug=0)
total_records = 50000
load_users = 1000
read_records = 100
redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)

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
        # self.run_mem(columns)
        self.run_redis(columns)

    def run_mem(self, columns):
        while(1):
            column_idx = random.randint(0, len(columns)-1)
            key = "stop_times." + str(random.randint(1, total_records)) + '.' + columns[column_idx]
            mc.get(key)

    def run_redis(self, columns):
        while(1):
            column_idx = random.randint(0, len(columns)-1)
            key = "stop_times." + str(random.randint(1, total_records)) + '.' + columns[column_idx]
            redis_db.get(key)

class AccessReportGenerator(threading.Thread):
    def __init__(self, users, record_limit):
        threading.Thread.__init__(self)
        self.record_limit = record_limit
        self.users = users

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
        # self.run_mem(columns)
        self.run_redis(columns)
    
    def run_mem(self, columns):
        output_file = 'outputs/memcached_accesstime_' + str(self.users) + '_' + str(self.record_limit) + '.csv'
        print(output_file)
        with open(output_file, 'w', newline='') as writefile:
            headers = ['index', 'time_taken']
            report_writer = csv.DictWriter(writefile, fieldnames=headers)
            report_writer.writeheader()
            misses = 0
            for i in range(100):
                start_time = time.time()
                for j in range(self.record_limit):
                    column_idx = random.randint(0, len(columns)-1)
                    key = "stop_times." + str(random.randint(1, total_records)) + '.' + columns[column_idx]
                    result = mc.get(key)
                    if result == '':
                        misses += 1
                time_delta = time.time() - start_time
                output_row = {
                    'index': i,
                    'time_taken': time_delta
                }
                print(output_row)
                report_writer.writerow(output_row)
            print('total misses: ',misses)

            

    def run_redis(self, columns):
        output_file = 'outputs/redis_accesstime_' + str(self.users) + '_' + str(self.record_limit) + '.csv'
        with open(output_file, 'w', newline='') as writefile:
            headers = ['index', 'time_taken']
            report_writer = csv.DictWriter(writefile, fieldnames=headers)
            report_writer.writeheader()
            for i in range(100):
                start_time = time.time()
                for j in range(self.record_limit):
                    column_idx = random.randint(0, len(columns)-1)
                    key = "stop_times." + str(random.randint(1, total_records)) + '.' + columns[column_idx]
                    redis_db.get(key)
                time_delta = time.time() - start_time
                output_row = {
                    'index': i,
                    'time_taken': time_delta
                }
                print(output_row)
                report_writer.writerow(output_row)


if __name__ == '__main__':
    for x in range(load_users):
        load_thread = AccessLoadGenerator(name = str(x + 1))
        load_thread.start()
    reporting_thead = AccessReportGenerator(load_users, read_records)
    reporting_thead.start()
