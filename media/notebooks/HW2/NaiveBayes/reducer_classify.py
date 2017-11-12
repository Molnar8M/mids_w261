#!/usr/bin/env python
from operator import itemgetter
import sys

numberOfRecords = 0
NumberOfMisclassifications=0
classificationAccuracy = 0

# START STUDENT CODE HW231REDUCER_CLASSIFY

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW2.3.1 Reducer Counters,Calls,1\n")

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    docID, docClass, predClass, nbVal, nonpredClass, nbValNP = line.split("\t")
    
    print line
    
    numberOfRecords += 1
    if docClass != predClass: NumberOfMisclassifications += 1
        
classificationAccuracy = float(numberOfRecords-NumberOfMisclassifications)/numberOfRecords
    
# END STUDENT CODE HW231REDUCER_CLASSIFY

print '\nMultinomial Naive Bayes Classifier Results are: records - %d, misclassifications - %d, classification accuracy - %f\n' % \
    (numberOfRecords, NumberOfMisclassifications, classificationAccuracy) 