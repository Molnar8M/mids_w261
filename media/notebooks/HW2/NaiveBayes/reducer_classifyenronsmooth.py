#!/usr/bin/env python
from operator import itemgetter
import sys

numberOfRecords = 0
NumberOfMisclassifications=0
classificationAccuracy = 0

# START STUDENT CODE HW24REDUCER_CLASSIFY

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW2.4 Reducer Counters,Calls,1\n")

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    docID, docClass, predClass, nbVal, nonpredClass, nbValNP = line.split("\t")
    
    numberOfRecords += 1
    if docClass != predClass: NumberOfMisclassifications += 1
        
misclassrate = float(NumberOfMisclassifications)/numberOfRecords
    
# END STUDENT CODE HW24REDUCER_CLASSIFY

print '\nMultinomial Naive Bayes Classifier Results are: records - %d, misclassifications - %d, misclass rate - %f\n' % \
    (numberOfRecords, NumberOfMisclassifications, misclassrate) 