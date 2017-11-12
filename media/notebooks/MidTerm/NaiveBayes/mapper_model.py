#!/usr/bin/python

'''
Stateless Naive Bayes. 

'''



import sys, re, string

# Init mapper phase 
# define regex for punctuation removal
regex = re.compile('[%s]' % re.escape(string.punctuation))

# inner loop mapper phase: process each record
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    # docID,docClass,title,body = line.split("\t", 3)
    parts = line.split("\t")
    docID, docClass, title = parts[0:3]
    if len(parts) == 4:
        body = parts[3]
    else:
        body = ""
    # use subject and body 
    # remove punctuations, only have white-space as delimiter
    emailStr = regex.sub(' ', title.lower() + " " +body.lower()) #replace each punctuation with a space
    emailStr = re.sub( '\s+', ' ', emailStr )            # replace multiple spaces with a space
    # split the line into words
    words = emailStr.split()
    
    # Append "ClassPriors" to get it added to the stream once per document
    # the reducer will do the math based on how many times it sees "ClassPrior"
    # for each doc type.
    words.append("ClassPriors")
    for w in words: 
        print "%s-%s\t%d" % ("z" + w, docClass, 1)
        if w != "ClassPriors":
            print "%s-%s\t%d" % ("**ClassWordCountTotals", docClass, 1)