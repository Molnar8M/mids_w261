#!/usr/bin/env python
# START STUDENT CODE HW213REDUCER

import sys

tot_count = 0

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW2.0.1 Reducer Counters,Calls,1\n")
for line in sys.stdin:
    key, value = line.split()
    tot_count += int(value)
    
print key, '\t', tot_count
    
# END STUDENT CODE HW213REDUCER