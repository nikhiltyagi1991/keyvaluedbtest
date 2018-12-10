import csv
import time
import pdb
import psutil
import redis
# import urllib.request

input_file = 'data/stop_times.csv'
output_file = 'outputs/redis_db_loading.csv'
items_count = 1000
write_miss = 0
write_limit = 50000

# redis_db = redis.StrictRedis(host="40.85.251.241", port=6379, db=0)
redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)

# key will be stop_times.trip_id.0
with open(output_file, 'w', newline='') as writefile:
    headers = ['record_index', 'timestamp', 'time_taken', 'cpu_usage', 'memory_usage', 'memory_free']
    # report_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    report_writer = csv.DictWriter(writefile, fieldnames=headers)
    report_writer.writeheader()

    with open(input_file, 'r') as inputfile:
        index = 0
        start_timestamp = time.time()
        stop_times_reader = csv.DictReader(inputfile, delimiter=',', quotechar='|')
        print('start time: ', start_timestamp)
        psutil.cpu_percent(interval=None)

        for row in stop_times_reader:
            for column_name in row.keys():
                try:
                    redis_db.set("stop_times."+ str(index)+ "." + column_name, row[column_name])
                except:
                    write_miss += 1
            
            if index % items_count == 0:
                delta_time = time.time() - start_timestamp
                # print("Time taken for inserting %d is (in s): %f"%(index+1, delta_time))
                output_row = {
                    'record_index': index,
                    'timestamp': time.time(),
                    'time_taken': delta_time,
                    'cpu_usage': psutil.cpu_percent(interval=None),
                    'memory_usage': psutil.virtual_memory().used/(1024*1024*1024),
                    'memory_free': psutil.virtual_memory().free/(1024*1024*1024)
                }
                # urllib.request.urlopen("http://40.85.251.241/").read()
                report_writer.writerow(output_row)
                start_timestamp = time.time()

            if write_limit != None and write_limit < index:
                break
            index += 1

print('write misses ', write_miss)
print('end time: ', time.time())
