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
NBModel = NaiveBayesModel("SPAM_Model_MNB.tsv") 

# define regex for punctuation removal
regex = re.compile('[%s]' % re.escape(string.punctuation))

# track number of -inf scores per class
inf_score = [0,0]
scores0 = []
scores1 = []

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

    #print words
    #NBModel.printModel()
    
    scores = NBModel.classifyInLogs(words)
    nonpredClass, vNP = min(enumerate(scores), key=itemgetter(1))
    predClass, vP = max(enumerate(scores), key=itemgetter(1))
    
    if scores[0] == float("-inf"): inf_score[0] += 1
    if scores[1] == float("-inf"): inf_score[1] += 1
    scores0.append(scores[0])
    scores1.append(scores[1])
    
    # put out elements to calclate accuracy for the reducer
    print '%s\t%s\t%s\t%.12f\t%s\t%.12f' % (docID, docClass, predClass, vP, nonpredClass, vNP)
    
#print inf_score
#print scores0
#print scores1

# END STUDENT CODE HW231MAPPER_CLASSIFY