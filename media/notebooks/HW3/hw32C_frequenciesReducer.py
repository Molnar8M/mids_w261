#!/usr/bin/env python
# START STUDENT CODE HW32CFREQREDUCER

import sys

cur_key = None
cur_count = 0
tot_count = 0
sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

for line in sys.stdin:
    key, value, freq = line.split()
    
    if key == '--ALL':
        tot_count += int(value)
        continue
        
    if key == cur_key:
        cur_count += int(value)
    else:
        if cur_key:
            print '%s\t%s\t%.3f' % (cur_key, cur_count, float(cur_count)/tot_count)
        cur_key = key
        cur_count = int(value)

print '%s\t%s\t%.3f' % (cur_key, cur_count, float(cur_count)/tot_count)

# END STUDENT CODE HW32CFREQREDUCER