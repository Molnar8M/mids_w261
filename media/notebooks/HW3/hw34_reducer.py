#!/usr/bin/env python

import sys

cur_key = None
cur_t2 = None
cur_count = 0
cur_total = 0
sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

for line in sys.stdin:
    key, value = line.split()
    t1, t2 = key.split(',')
    if key == cur_key:
        # sum up the marginal or the co-occurance counts
        if cur_t2 == '*':
            cur_total += int(value)
        else:
            cur_count += int(value)
    else:
        if cur_key and cur_count > 100  and cur_t2 != '*':
            print '%s\t%s\t%s\t%.3f' % (cur_key, cur_count, cur_total, float(cur_count)/cur_total)
            
        # init either a marginal or co-occurance count
        if t2 == '*':
            cur_total = int(value)
        else:
            cur_count = int(value)
        # init the key
        cur_key = key
        cur_t2 = t2
        
if cur_key and cur_count > 100  and cur_t2 != '*':
    print '%s\t%s\t%s\t%.3f' % (cur_key, cur_count, cur_total, float(cur_count)/cur_total)