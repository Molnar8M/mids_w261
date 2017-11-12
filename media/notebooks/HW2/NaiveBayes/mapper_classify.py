#!/usr/bin/env python
import sys
sys.path.append('./')
from NaiveBayesModel import NaiveBayesModel
import re, string
from operator import itemgetter

# Init mapper phase 

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW2.3.1 Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:processing my message...how are you\n")

# read the MODEL into memory
# The model file resides the local disk (make sure to ship it home from HDFS).
# In the Hadoop command linke be sure to add the follow the -files commmand line option
NBModel = NaiveBayesModel("NaiveBayes.txt") 

# define regex for punctuation removal
regex = re.compile('[%s]' % re.escape(string.punctuation))

# inner loop mapper phase: process each record
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    parts = line.split("\t")
    docID, docClass, title = parts[0:3]
    if len(parts) == 4:
        body = parts[3]
    else:
        body = ""
    # use subject and body 
    # remove punctuations, only have white-space as delimiter
    emailStr = regex.sub(' ', title.lower() + " " + body.lower()) #replace each punctuation with a space
    emailStr = re.sub( '\s+', ' ', emailStr )            # replace multiple spaces with a space
    # split the line into words
    words = emailStr.split()

# START STUDENT CODE HW231MAPPER_CLASSIFY

    #predClass, v = max(enumerate(NBModel.classifyInLogs(words)), key=itemgetter(1))
    predClass, vP = max(enumerate(NBModel.classify(words)), key=itemgetter(1))
    nonpredClass, vNP = min(enumerate(NBModel.classify(words)), key=itemgetter(1))
    #print predClass, v
    
    # put out elements to calclate accuracy for the reducer
    print '%s\t%s\t%s\t%.12f\t%s\t%.12f' % (docID, docClass, predClass, vP, nonpredClass, vNP)
    
# END STUDENT CODE HW231MAPPER_CLASSIFY