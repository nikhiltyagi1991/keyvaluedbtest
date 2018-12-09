import memcache
import csv
import time
import pdb
import psutil

input_file = 'data/stop_times.csv'
output_file = 'outputs/memcached_db_loading.csv'
items_count = 1000
mc = memcache.Client(['127.0.0.1:11211'], debug=0)

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
                storage_key = "stop_times."+ str(index)+ "." + column_name
                mc.set(storage_key, row[column_name])
                if index % items_count == 0:
                    print(storage_key)
            
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
                report_writer.writerow(output_row)
                start_timestamp = time.time()

            if limit != None and index > limit:
                break
            index += 1
        
        print('end time: ', time.time())
