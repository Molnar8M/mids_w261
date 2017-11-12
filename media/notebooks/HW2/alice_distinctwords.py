# Count the number of unique words in the Alice Book in a singlethreaded manner
import re
import sys
from collections import defaultdict

pathToFile = sys.argv[1]
wordCounts = defaultdict(int)

def hw211(pathToFile):
    
    # run the distinct word count, ignoring case, over the file entered on the command line
    with open (pathToFile, "r") as myfile:
        for line in myfile.readlines():
            # changed out line.lower().split() for familiar word pattern matching
            for word in re.findall(r"\b[a-zA-Z]+['-]?[a-zA-Z]*\b", line.lower()):
                wordCounts[word] += 1
    
    print 'Number of distinct words: %d' % len(wordCounts.keys())

hw211(pathToFile)