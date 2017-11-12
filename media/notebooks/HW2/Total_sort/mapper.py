#!/usr/bin/env python
# START STUDENT CODE HW212MAPPER

import sys
import re
from collections import defaultdict

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW2.1 Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:processing my message...how are you\n")

wordCounts = defaultdict(int)

for line in sys.stdin:
    # change out line.lower().split() for familiar word pattern matching
    for word in re.findall(r"\b[a-zA-Z]+['-]?[a-zA-Z]*\b", line.lower()):
        wordCounts[word] += 1
        
#    for word in line.lower().split():
#        wordCounts[word] += 1

for key, value in wordCounts.items():
    print key, '\t', value
    
# END STUDENT CODE HW212MAPPER