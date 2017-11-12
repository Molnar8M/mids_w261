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

class MRsimilarity(MRJob):
  
    #START SUDENT CODE531_SIMILARITY

    MRJob.SORT_VALUES = True 
    
    def steps(self):
        JOBCONF_STEP1 = {}
        JOBCONF_STEP2 = { 
            ######### IMPORTANT: THIS WILL HAVE NO EFFECT IN -r local MODE. MUST USE -r hadoop FOR SORTING #############
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options':'-k1,1nr',
        }
        ######### MODIFIED ARGUEMENTS TO WORK FOR -r local AS -r hadoop BLOWS UP WITH CLOUDERA DOCKER
        JOBCONF = {
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k1,1nr', #''-k1 -k2nr',
            "mapred.reduce.tasks":1
        }
        return [MRStep(jobconf = JOBCONF_STEP1,
                    mapper = self.mapper_pair_sim,
                    reducer = self.reducer_pair_sim,
                ),
                MRStep(jobconf = JOBCONF,
                    mapper = None,   
                    reducer = self.reducer_sort
                )
        ]
    
    # purpose: break apart the doc mappings and calculate a partial similarity for each pair
    def mapper_pair_sim(self, _, line):
        line = line.strip()
        index, posting = line.split("\t")
        posting = json.loads(posting)
        
        '''
        @input: lines (postings) from inverted index
         "blue" [["DocA", 4], ["DocC", 4], ["DocE", 3]]
        '''
        
        # build out all of the document pairs for the line
        for subset in itertools.combinations(posting, 2):
            # output the values necessary for jaccard calculation
            yield (subset[0][0], subset[1][0]), (1, subset[0][1], (subset[1][1]))


    # purpose: sum the partial similarities
    def reducer_pair_sim(self, key, value):
        
        inter_cnt = 0
        d = {}
        
        # sum for final jaccard values
        final_key = key[0] + ' - ' + key[1]
        for i in value:
            inter_cnt += 1
            doc1_len = i[1]
            doc2_len = i[2]
        jaccard = inter_cnt / (doc1_len + doc2_len - inter_cnt)
        d['cosine'] = None
        d['jaccard'] = jaccard
        d['overlap'] = None
        d['dice'] = None

        yield jaccard, (final_key, d)
    
    # purpose: sort the final output by the avg or other dist value
    def reducer_sort(self, key, value):
        for v in value:
            yield key, v

  #END SUDENT CODE531_SIMILARITY
  
if __name__ == '__main__':
    MRsimilarity.run()