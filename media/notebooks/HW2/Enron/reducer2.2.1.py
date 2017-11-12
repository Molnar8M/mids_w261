#!/usr/bin/env python
from operator import itemgetter
import sys

# START STUDENT CODE HW221REDUCER

cur_key = None
cur_count = 0

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW2.2.1 Reducer Counters,Calls,1\n")

for line in sys.stdin:
    key, word, docClass, value = line.split()
    if key == cur_key:
        cur_count += int(value)
    else:
        if cur_key:
            print '%s\t%s\t%s' % (cur_key.split(',')[0], cur_key.split(',')[1], cur_count)
        cur_key = key
        cur_count = int(value) 
            
print '%s\t%s\t%s' % (cur_key.split(',')[0], cur_key.split(',')[1], cur_count)   

# END STUDENT CODE HW221REDUCER