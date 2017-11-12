#!/usr/bin/env python
from operator import itemgetter
import sys

# START STUDENT CODE HW221REDUCER

cur_key = None
cur_count = 0

size = 10
tops = [[],[]]

def top_lists (i, c, w, cnt):
    # check the list length then act
    if len(tops[i]) < size: 
        tops[i].append((w, cnt))
    else:
        # check min value
        tops[i] = sorted(tops[i], key=itemgetter(1), reverse=True)
        # replace the min tuple if the count is greater than the min tuple
        if cnt > tops[i][-1][1]:
            tops[i][-1] = (w, cnt)

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW2.2.2 Reducer Counters,Calls,1\n")

for line in sys.stdin:
    key, word, docClass, value = line.split()
    if key == cur_key:
        cur_count += int(value)
    else:
        if cur_key:
            # manage top lists for the doc classes
            wd, cls = cur_key.split(',')
            top_lists(int(cls), cls, wd, cur_count)
        cur_key = key
        cur_count = int(value) 

wd, cls = cur_key.split(',')
top_lists(int(cls), cls, wd, cur_count)
         
for idx, lst in enumerate(tops):
    for wd, cnt in lst:
        print '%s\t%s\t%s' % (idx, wd, cnt)

# END STUDENT CODE HW221REDUCER