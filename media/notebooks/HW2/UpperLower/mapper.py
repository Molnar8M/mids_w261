#!/usr/bin/env python

import sys
import re

#sys.stderr.write("reporter:counter:Tokens,Total,1") # NOTE missing the carriage return so wont work
# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW2.1 Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:processing my message...how are you\n")

# START STUDENT CODE HW21MAPPER

upper = 0
lower = 0
    
for line in sys.stdin:
    # replaced line.split() with findall for more discrete word matching
    for word in re.findall(r"\b[a-zA-Z]+['-]?[a-zA-Z]*\b", line):
        if word[0].isupper():
            upper += 1
        elif word[0].islower():
            lower += 1

print '%s\t%s' % ('lower', lower)
print '%s\t%s' % ('upper', upper)

# END STUDENT CODE HW21MAPPER