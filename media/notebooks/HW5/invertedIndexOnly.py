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

class MRinvertedIndexOnly(MRJob):
    
    MRJob.SORT_VALUES = True 
    
    def steps(self):

        return [MRStep(
                    mapper=self.mapper,
                    reducer=self.reducer)
                ]
    
        
   
    def mapper(self,_,line):
        '''
        Reference:
        "For each term in the document,emits the term as key, and a tuple 
        consisting of the doc id and term weight as the value.
        The MR runtime automatically handles the grouping of these tuples..."
        (https://terpconnect.umd.edu/~oard/pdf/acl08elsayed2.pdf)
        '''
        #####################################################################
        # docs as input, ie:
        # doc id \t doc value
        # docA	bright blue butterfly forget
        #
        #####################################################################
        
        ## MAPPER CODE HERE
        
        
    def reducer(self,key,value):
        '''
        Reference con't:
        "...which the reducer then writes out to disk, thus generating the postings."
        (https://terpconnect.umd.edu/~oard/pdf/acl08elsayed2.pdf)
        '''
        #####################################################################
        # Inverted Index as output, ie:
        # "term" [["doc",doc_length]]
        # "butterfly"	[["docA", 4],["docD", 4],["docE", 3]]
        #####################################################################
        
        ## REDUCER CODE HERE
        
if __name__ == '__main__':
    MRinvertedIndexOnly.run() 