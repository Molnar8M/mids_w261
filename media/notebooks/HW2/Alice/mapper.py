#!/usr/bin/env python
# START STUDENT CODE HW213MAPPER

import sys
import re
from collections import defaultdict

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW2.1 Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:processing my message...how are you\n")

wordCounts = defaultdict(int)

findword = sys.argv[1].lower()
count = 0

for line in sys.stdin:
    # changed out line.lower().split() for familiar word pattern matching
    for word in re.findall(r'\b%s\b' % findword, line.lower()):
        count += 1

print findword, '\t', count

# END STUDENT CODE HW213MAPPER