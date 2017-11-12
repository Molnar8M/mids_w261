#!/usr/bin/env python
# START STUDENT CODE HW31REDUCER

import sys

cur_key = None
cur_count = 0
sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

for line in sys.stdin:
    key, value = line.split('\t')
    if key == cur_key:
        cur_count += int(value)
    else:
        if cur_key:
            print '%s\t%s' % (cur_key, cur_count)
        cur_key = key
        cur_count = int(value)

print '%s\t%s' % (cur_key, cur_count)

# END STUDENT CODE HW31REDUCER