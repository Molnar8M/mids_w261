#!/usr/bin/python
import sys
import re
count = 0
WORD_RE = re.compile(r"[\w']+")
filename = sys.argv[2]
findword = sys.argv[1]
with open (filename, "r") as myfile:
    for line in myfile.readlines():
        # The if and re.search statement below will count the lines containing the findword ignoring case. 
        # The output will yield the same results when comparing to the grep command. Use re.findall if one 
        # wants to count all instances of the findword match.
        if re.search(findword, line, flags = re.I):
            num = 1
        else:
            num = 0
        #print(num)
        if num: count = count + num  
print(count)