#!/usr/bin/env python

import sys

cur_key = None
cur_count = 0
tot_count = 0
sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

for line in sys.stdin:
    key, value, total = line.split()
    if key == cur_key:
        cur_count += int(value)
        tot_count += int(total)
    else:
        if cur_key:
            print '%s\t%s\t%.3f' % (cur_key, cur_count, float(cur_count)/tot_count)
        cur_key = key
        cur_count = int(value)
        tot_count = int(total)

print '%s\t%s\t%.3f' % (cur_key, cur_count, float(cur_count)/tot_count)