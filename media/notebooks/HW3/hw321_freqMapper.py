#!/usr/bin/env python
import sys

sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:hw321_totalMapper running\n")

tot_count = 0

for line in sys.stdin:
    values = line.split()
    print '%s\t%s' % (values[0], values[1])
    tot_count += int(values[1])
    
print '%s\t%s' % ('--ALL', tot_count)