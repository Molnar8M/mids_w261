#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

from __future__ import division
import collections
import re
import json
import math
import numpy as np
import itertools
import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys, ast

class MRinvertedIndex(MRJob):
    
    #START SUDENT CODE531_INV_INDEX
    
    MRJob.SORT_VALUES = True
    
    def steps(self):
        # restrict to one reducer so output is sorted; change this later
        JOBCONF_STEP = {
          "mapred.reduce.tasks":1
        }
        return [
            MRStep(jobconf = JOBCONF_STEP,
                    mapper = self.mapper,
                   reducer = self.reducer,
            )
        ]  

    # purpose: split out the word pair counts for the ngrams
    # input: line from stripes file
    # output: key(word in ngram), value(doc and doc lengths)
    def mapper(self, _, line):
        # splice out the input from the stripes file      
        items = line.strip().split('\t')
        doc = items[0].replace("\"","")
        stripe = ast.literal_eval(items[1])
        # determine the ngram length and output
        length = len(stripe.keys())
        for word, cnt in stripe.iteritems():
            yield word, (doc, length)
    
    # purpose: take the sorted input and prep for output
    # input: key(word in ngram), value(doc and doc lengths)
    # output: key(word in ngram), value(list of doc and doc lengths)
    def reducer(self, word, doc_len_list):
        doc_lens = [i for i in doc_len_list]
        yield word, doc_lens    

    #END SUDENT CODE531_INV_INDEX 

if __name__ == '__main__':
    MRinvertedIndex.run() 