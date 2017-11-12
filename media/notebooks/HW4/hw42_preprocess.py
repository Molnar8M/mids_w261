#!/usr/bin/env python
#START STUDENT CODE42
import sys

cur_id = None

for line in sys.stdin:
    l = line.strip().split(',')
    if l[0] == 'C':
        cur_id = l[2]
    else:
        print '%s,%s,%s,%s,%s' % ('V', l[1], 1, 'C', cur_id)

#END STUDENT CODE42