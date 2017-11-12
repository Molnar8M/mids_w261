#!/usr/bin/env python
# START STUDENT CODE HW32AMAPPER

import sys

sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:hw32A_mapper running\n")

for line in sys.stdin:
    for word in line.split():
        sys.stderr.write("reporter:counter:Token Counters,Calls,1\n")
        print '%s\t%s' % (word, 1) 
            
# END STUDENT CODE HW32AMAPPER