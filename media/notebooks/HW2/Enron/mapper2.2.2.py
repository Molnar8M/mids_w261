#!/usr/bin/env python
import sys, re, string

# START STUDENT CODE HW222MAPPER

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW2.2.2 Mapper Counters,Calls,1\n")
sys.stderr.write("reporter:status:processing my message...how are you\n")

# define regex for punctuation removal
regex = re.compile('[%s]' % re.escape(string.punctuation))

# input comes from STDIN (standard input)
for line in sys.stdin:
    docID, docClass, subject, body = line.split("\t", 3)

    # use subject and body
    # remove punctuations, only have white-space as delimiter
    emailStr = regex.sub(' ', subject + " " + body.lower())
    emailStr = re.sub('\s+', ' ', emailStr)
    
    # write the results to STDOUT (standard output);
    # what we output here will be the input for the
    # Reduce step, i.e. the input for reducer.py
    #
    # tab-delimited; the trivial word count is 1

    words = emailStr.split()
    for w in words: 
        print w+','+docClass, '\t', w, '\t', docClass, '\t', 1
            
# END STUDENT CODE HW222MAPPER            