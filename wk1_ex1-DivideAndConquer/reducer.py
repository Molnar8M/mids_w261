#!/usr/bin/python
import sys
sum = 0
for line in sys.stdin:
    # Convert string to an int and sum the running total.
    #print int(line)
    sum = sum + int(line)
print(sum)