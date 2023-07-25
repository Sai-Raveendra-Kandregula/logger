import csv
import os
import re
import datetime
import statistics
from dataclasses import dataclass
from os import listdir

def find_csv_filenames( path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]

raw_files_directory = "results_raw"
raw_files = find_csv_filenames(raw_files_directory)
raw_data = []
# print(raw_files)

@dataclass
class Stats:
    producers_count : int
    msg_count : int
    mean_delta : float
    min_delta : int
    max_delta : int
    sd_delta : float
    isCPUBound : bool = False

    def __iter__(self):
        return iter([self.producers_count, self.msg_count, self.isCPUBound, self.mean_delta, self.min_delta, self.max_delta, self.sd_delta])

results = []

for filename in raw_files:
    with open(os.path.join(raw_files_directory, filename), mode ='r')as file:
        csvFile = csv.reader(file)
        file_info = re.findall(r'\d+', filename) # return [pro_num, msg_num]
        producer_values = []
        broker_values = []
        deltas = [] # deltas in nanoseconds
        for line in csvFile:
            producer_values.append(datetime.datetime.strptime(line[0][:-3], '%m/%d/%y %H:%M:%S.%f')) # stripping data under microsecond precision
            broker_values.append(datetime.datetime.strptime(line[1][:-3], '%m/%d/%y %H:%M:%S.%f')) # stripping data under microsecond precision
        for i in range(0, len(producer_values)):
            delta = (broker_values[i] - producer_values[i])
            deltas.append(delta.seconds*pow(10,6) + delta.microseconds)
        print("Processing : " + filename)
        min_val = min(deltas)
        max_val = max(deltas)
        mean_val = statistics.mean(deltas)
        sd_val = statistics.stdev(deltas)
        results.append( Stats(file_info[0], file_info[1], mean_val, min_val, max_val, sd_val, filename.__contains__("bound")) )

result_filename = datetime.datetime.now().strftime("results/results_%Y-%m-%d %H:%M:%S.csv");

with open(result_filename, mode ='w') as result_file:
    csvwriter = csv.writer(result_file)
    csvwriter.writerow(["producers_count", "msg_count", "isCPUBound", "mean_delta", "min_delta", "max_delta", "sd_delta"])

    csvwriter.writerows( [ list(x) for x in results ] )

print("Processed Data Written to " + result_filename)