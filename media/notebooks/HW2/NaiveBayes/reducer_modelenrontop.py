#!/usr/bin/env python
import sys, re, string

# START STUDENT CODE HW233MAPPER

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW2.2.3 Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:processing my message...how are you\n")

# input comes from STDIN (standard input)
for line in sys.stdin: 
    print line
            
# END STUDENT CODE HW233MAPPER