#!/usr/bin/env python
# START STUDENT CODE HW32CFREQMAPPER

import sys, re, string
from collections import defaultdict

sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:hw32C_frequenciesMapper running\n")

regex = re.compile('[%s]' % re.escape(string.punctuation))
WORD_RE = re.compile(r"[\w']+")

words = defaultdict(int)

for line in sys.stdin:
    values = line.split(',')
    issueStr = regex.sub('', values[3].lower())
    for word in WORD_RE.findall(issueStr):
        sys.stderr.write("reporter:counter:Token Counters,Calls,1\n")
        words[word] += 1

print '%s\t%s\t%s' % ('--ALL', sum(words.values()), 0)
for k in words.keys():
    print '%s\t%s\t%s' % (k, words[k], 0) 

# END STUDENT CODE HW32CFREQMAPPER