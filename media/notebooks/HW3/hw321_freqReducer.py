#!/usr/bin/env python
import sys, re, string

sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")
sys.stderr.write("reporter:status:hw321_freqReducer running\n")

tot_count = 0

for line in sys.stdin:
    values = line.split()
    if values[0] == '--ALL':
        tot_count += int(values[1])
        sys.stderr.write("reporter:status:hw321_freqReducer tot_count=%s\n" % tot_count)
        continue
    print '%s\t%s\t%s' % (values[0], values[1], float(values[1])/tot_count)