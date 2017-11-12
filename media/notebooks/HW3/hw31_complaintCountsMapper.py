#!/usr/bin/env python
# START STUDENT CODE HW31MAPPER

import sys

sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:hw31_complaintCountsMapper running\n")

for line in sys.stdin:
    values = line.split(',')
    print '%s\t%s' % (values[1], 1)
    if values[1] == "Debt collection":
        sys.stderr.write("reporter:counter:Debt collection Counters,Calls,1\n")  
    elif values[1] == "Mortgage":
        sys.stderr.write("reporter:counter:Mortgage Counters,Calls,1\n") 
    else:
        sys.stderr.write("reporter:counter:Other Counters,Calls,1\n")

# END STUDENT CODE HW31MAPPER