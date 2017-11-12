#!/usr/bin/env python
from operator import itemgetter
import sys, operator

# START STUDENT CODE HW231REDUCER_MODEL

smooth_factor = 0 # no smoothing
doc_count = [smooth_factor, smooth_factor]
cls_word_count = [smooth_factor, smooth_factor]
wordcount = {}
docIDs = {}
cur_key = None
cur_count = 0

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW2.3.1 Reducer Counters,Calls,1\n")

for line in sys.stdin:
    key, word, docClass, docID, value = line.split()
    docIDs[docID] = docClass
    
    if key == cur_key:
        cur_count += int(value)
    else:
        if cur_key:   
            # store total words in each docClass
            w, cls = cur_key.split(',')
            cls_word_count[int(cls)] += cur_count            
            # store word freq per class 
            wordcount[w][int(cls)] = cur_count

        cur_key = key
        cur_count = int(value) 
        # track counts per class for each word
        if word not in wordcount.keys(): wordcount[word] = [0,0] 
        
# store total words in each docClass
w, cls = cur_key.split(',')
cls_word_count[int(cls)] += cur_count
# store word freq per class 
wordcount[w][int(cls)] = cur_count

# use the reducer to output the model text file

# output the conditional properties
v = len(wordcount.keys())
for k, [v0, v1] in wordcount.iteritems():
    # freq of word in class 0
    fv0 = float(v0)
    # freq of word in class 1
    fv1 = float(v1)
    # P(w|c = 0)
    # smoothing option: pr0 = (fv0 + 1)/(cls_word_count[0] + v)
    pr0 = fv0/cls_word_count[0]
    # P(w|c = 1)
    # smoothing option: pr1 = (fv1 + 1)/(cls_word_count[1] + v)
    pr1 = fv1/cls_word_count[1]
    #out_file.write('%s\t%.1f,%.1f,%.12f,%.12f\n' % (k, fv0, fv1, pr0, pr1))
    print '%s\t%.1f,%.1f,%.12f,%.12f' % (k, fv0, fv1, pr0, pr1)

# output the prior probabilities
c0 = docIDs.values().count('0')
c1 = docIDs.values().count('1')
n = len(docIDs.keys())
p0 = float(c0)/n
p1 = float(c1)/n
print '%s\t%.1f,%.1f,%.2f,%.2f' % ('ClassPriors', c0, c1, p0, p1)

# END STUDENT CODE HW231REDUCER_MODEL 