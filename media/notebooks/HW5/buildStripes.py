#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

from __future__ import division
import re
import mrjob
import json
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys, itertools
from collections import Counter

class MRbuildStripes(MRJob):
  
    #START SUDENT CODE531_STRIPES
  
    MRJob.SORT_VALUES = True
    
    # purpose: only need a mapper to coordinate ngram pairs with the count and reducer to combine   
    def steps(self):
        # restrict to one reducer so output is sorted; change this later
        JOBCONF_STEP = {
          "mapred.reduce.tasks":1
        }
        return [
            MRStep(jobconf = JOBCONF_STEP,
                   mapper=self.mapper,
                   reducer=self.reducer,
            )
        ]
    
    # purpose: split out the word pair counts for the ngrams
    # input: line from ngram input file - ngram \t count \t page count \t book count
    # output: key(word in ngram), value(dict of remaining ngram words with counts)
    def mapper(self, _, line):
        line = line.strip().split('\t')
        # capture the n-gram words
        words = line[0].lower().split()        
        # capture the count, pages_count, books_count values; only keep the count for use
        count = int(line[1:][0])
        # handle any ngrams with duplicate words
        total_inst = dict(Counter(words))
        # init the dictionary for emitting
        H = {}
        
        # note the set will remove duplicate words in the ngram list - how should this be handled?
        for subset in itertools.combinations(sorted(set(words)), 2):
            # check the first in the pair and only keep the count
            if subset[0] not in H.keys():
                H[subset[0]] = {}
                H[subset[0]][subset[1]] = total_inst[subset[1]] * count
            elif subset[1] not in H[subset[0]]:
                H[subset[0]][subset[1]] = total_inst[subset[1]] * count
            else:
                H[subset[0]][subset[1]] += (total_inst[subset[1]] * count)
                
            # check the second in the pair to capture the symmetry relationship
            if subset[1] not in H.keys():
                H[subset[1]] = {}
                H[subset[1]][subset[0]] = total_inst[subset[1]] * count
            elif subset[0] not in H[subset[1]]:
                H[subset[1]][subset[0]] = total_inst[subset[1]] * count
            else:
                H[subset[1]][subset[0]] += (total_inst[subset[1]] * count)
 
        for key in H.keys():
            yield key, (H[key])
    
    # purpose: combine the sorted-by-key mapper outputs into stripes
    # input: key(word in ngram), value(dict of remaining ngram words with counts)
    # output: key(word in ngram), value(dict of remaining ngram words with total counts)
    def reducer(self, key, value):
        d = Counter()
        for item in value:
            item = {k:int(v) for k, v in item.iteritems()}
            d = d + Counter(item)
        yield key, dict(d)
        
    #END SUDENT CODE531_STRIPES

if __name__ == '__main__':
    MRbuildStripes.run()