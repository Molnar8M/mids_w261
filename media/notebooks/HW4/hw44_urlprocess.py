#!/usr/bin/env python
import sys

base_url = None

for line in sys.stdin:
    l = line.strip().split(',')
    if l[0] == 'I':
        base_url = l[2].replace('"','')
    else:
        print '%s,%s' % (l[1], base_url+l[4].replace('"',''))