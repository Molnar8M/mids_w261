#!/usr/bin/env python

import sys
import itertools
from collections import defaultdict
from operator import itemgetter
import numpy as np

sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:hw34_mapper running\n")

products = defaultdict(int)

for line in sys.stdin:
    #print line
    items = line.split()
    #print sorted(items)
    for key in list(itertools.combinations(sorted(items), 2)):
        products[key] += 1
        print '%s,%s\t%s' % (key[0], key[1], 1)
        
# capture totals 
for i in np.unique(list(k[0] for k in products.keys())):
    s = sum(v for k,v in products.iteritems() if i in k[0])
    print '%s,%s\t%s' % (i, "*", s)