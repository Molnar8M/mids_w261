#!/usr/bin/env python

import sys, operator
from collections import defaultdict

sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:hw33_freqMapper running\n")

products = defaultdict(int)

for line in sys.stdin:
    items = line.split()
    for i in items:
        sys.stderr.write("reporter:counter:Token Counters,Calls,1\n")
        products[i] += 1

tot_count = sum(products.values())
for item, freq in sorted(products.items(), key=operator.itemgetter(1), reverse = True)[0:50]:
    print '%s\t%s\t%s' % (item, freq, tot_count)