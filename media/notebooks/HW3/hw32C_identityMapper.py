#!/usr/bin/env python
# START STUDENT CODE HW32CIDMAPPER

import sys, re, string

sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:hw32C_identityMapper running\n")

for line in sys.stdin:
    values = line.split()
    print '%s\t%s\t%s' % (values[0], values[1], values[2])
    
# END STUDENT CODE HW32CIDMAPPER