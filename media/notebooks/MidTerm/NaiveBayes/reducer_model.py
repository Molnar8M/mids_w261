#!/usr/bin/python
from operator import itemgetter
import sys, operator
#import numpy as np
from collections import defaultdict

def print_output_line(totals, current_counts, word):
    hamFreq = current_counts[0]
    spamFreq = current_counts[1]
    if word != "ClassPriors":
        probWordGivenSpam = spamFreq / totals[1]
        probWordGivenHam = hamFreq / totals[0]
        print "%s\t%d,%d,%f,%f" % (word, hamFreq, spamFreq, probWordGivenHam, probWordGivenSpam)
    else:
        probSpam = spamFreq / (spamFreq + hamFreq)
        probHam = hamFreq / (spamFreq + hamFreq)
        print "%s\t%d,%d,%f,%f" % (word, hamFreq, spamFreq, probHam, probSpam)  
        
totals = [0, 0]
current_counts = [0.0, 0.0]
prev_word = None
for line in sys.stdin:
    id, value = line.split()
    word = id[0]
    word, docClass = id.split("-")
    docClass = int(docClass)
    value = int(value)
    
    if word == "**ClassWordCountTotals":
        totals[docClass] += value
    else:
        word = word[1:]
        if prev_word == "**ClassWordCountTotals":
            prev_word = word
        if word != prev_word:
            print_output_line(totals, current_counts, prev_word)
            current_counts = [0.0,0.0]
            
        current_counts[docClass] += value

    prev_word = word
      
print_output_line(totals, current_counts, word)  