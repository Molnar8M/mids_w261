#!/usr/bin/env python
# START STUDENT CODE HW32BMAPPER

import sys, re, string

sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:hw32B_mapper running\n")

regex = re.compile('[%s]' % re.escape(string.punctuation))

for line in sys.stdin:
    values = line.split(',')
    issueStr = regex.sub('', values[3].lower())
    #print issueStr
    for word in issueStr.split():
        sys.stderr.write("reporter:counter:Token Counters,Calls,1\n")
        print '%s\t%s' % (word, 1) 
        
# END STUDENT CODE HW32BMAPPER