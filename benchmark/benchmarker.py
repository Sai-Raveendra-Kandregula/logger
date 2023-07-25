import csv
import os
import datetime
import shutil
import subprocess

executable_path = "./log_benchmarker_cli --np %d --nm %d %s"
configuration_file = "benchmark_conf.csv"
postprocessor = "python3 'result_processor.py'"

x = datetime.datetime.now()
log_old_records_dest = x.strftime("results_raw_%d-%m-%Y_%H-%M-%S")

if (os.path.exists("results_raw")):
	shutil.move("results_raw", log_old_records_dest)
os.makedirs("results_raw")

with open(configuration_file, "r") as conf_file:
	csvreader = csv.reader(conf_file)
	line_count = 0
	for line in csvreader:
		if(line_count == 0): # Skip the Headers
			line_count += 1
			continue
		procnt = int(line[0])
		msgcnt = int(line[1])
                cpu_bind = "--bind" if (line[2] == "y" or line[2] == "Y") else ""
		cmd = executable_path %(procnt, msgcnt, cpu_bind)
		# print(cmd)
		# os.system( cmd )
		subprocess.check_call( cmd, shell = True )
		line_count += 1

os.system( postprocessor )
