#!/usr/bin/env python
# START STUDENT CODE HW211REDUCER

import sys
from collections import OrderedDict

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW2.1 Reducer Counters,Calls,1\n")

wordCounts = OrderedDict()
cur_key = None
cur_count = 0

for line in sys.stdin:
    key, value = line.split()
    if key == cur_key:
        wordCounts[cur_key] += int(value)
    else:
        cur_key = key
        wordCounts[cur_key] = int(value)
        
print len(wordCounts.keys())

# END STUDENT CODE HW211REDUCER