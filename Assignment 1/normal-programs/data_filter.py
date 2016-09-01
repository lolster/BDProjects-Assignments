from dateutil import parser
from datetime import datetime
from os import listdir
from os.path import isfile

MIN_DATE = parser.parse('2014-08-01T00:00:00')
MAX_DATE = parser.parse('2014-08-31T23:59:59')

#obtain list of all files in directory
files = [f for f in listdir() if isfile(f) and f != 'data_filter.py']

files_done = 0

for f in files:
	#remove first lines of csv and get TIMESTAMP POSITION
	csv_file = open(f, 'r')
	header = csv_file.readline().strip()
	header_split = header.split(',')
	#print(header, header_split)
	timestamp_index = header_split.index('TIMESTAMP')
	output_file = open('.\filtered\filtered_' + f, 'w')
	print(header, file=output_file)
	for l in csv_file.readlines():
		#read each line
		#convert the TIMESTAMP to datetime object
		#compare object with MIN_DATE and MAX_DATE
		#if between, print in output file
		l_split = l.strip().split(',')
		date_row = parser.parse(l_split[timestamp_index])
		if MIN_DATE <= date_row <= MAX_DATE:
			print(l, file=output_file, end='')
	print('done with', f)
	files_done += 1

print('files done:', files_done)
