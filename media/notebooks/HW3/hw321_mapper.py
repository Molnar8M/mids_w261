#!/usr/bin/env python
import sys, re, string

sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:hw32C_mapper running\n")

regex = re.compile('[%s]' % re.escape(string.punctuation))
WORD_RE = re.compile(r"[\w']+")

for line in sys.stdin:
    values = line.split(',')
    issueStr = regex.sub('', values[3].lower())
    #print issueStr
    for word in WORD_RE.findall(issueStr):
        sys.stderr.write("reporter:counter:Token Counters,Calls,1\n")
        print '%s\t%s' % (word, 1) 