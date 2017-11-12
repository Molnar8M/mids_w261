#!/usr/bin/env python
import sys, re, string

# Init mapper phase 
# define regex for punctuation removal
regex = re.compile('[%s]' % re.escape(string.punctuation))

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW2.4 Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:processing my message...how are you\n")

# inner loop mapper phase: process each record
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    # use subject and body 
    
    parts = line.split("\t")
    docID, docClass, title = parts[0:3]
    if len(parts) == 4:
        body = parts[3]
    else:
        body = ""
    
    # remove punctuations, only have white-space as delimiter
    emailStr = regex.sub(' ', title.lower() + " " + body.lower()) #replace each punctuation with a space
    emailStr = re.sub( '\s+', ' ', emailStr )            # replace multiple spaces with a space
    # split the line into words
    words = emailStr.split()

# START STUDENT CODE HW24MAPPER_MODEL

    for w in words: 
        print w+','+docClass, '\t', w, '\t', docClass, '\t', docID, '\t', 1

# increase counters
# write the results to STDOUT (standard output);
# what we output here will be the input for the
# Reduce step, i.e. the input for reducer.py
#
# tab-delimited; the trivial word count is 1
        
# END STUDENT CODE HW24MAPPER_MODEL            