#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

import re

import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict

class mostFrequentWords(MRJob):
    
    # START STUDENT CODE 5.4.1.B
    
    MRJob.SORT_VALUES = True
    
    # purpose: steps needed to find the top 10 most frequent words
    def steps(self):
        JOBCONF_STEP = {
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options':'-k1,1',
        }
        JOBCONF_STEP2 = {
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options':'-k1,1nr',
        }
        
        return [
            MRStep(jobconf = JOBCONF_STEP,
                   mapper_init = self.mapper_init,
                   mapper = self.mapper,
                   mapper_final = self.mapper_final,
                   combiner = self.combiner,
                   reducer = self.reducer,
            ),
            MRStep(jobconf = JOBCONF_STEP2,
                   mapper = None,
                   reducer = self.reducer_sort,  
            ),
        ]
    
    # purpose: define the mapper's dict used to track word freq
    def mapper_init(self):
        self.word_counts = defaultdict(int)
        #self.stopWords = set(stopwords.words('english'))
        self.stopWords = [sw for sw in open('english').read().strip().split('\n')]
        #print self.stopWords
        #self.stopWords = ['a']
        #self.stopWords = set([u'all', u'just', u'being', u'over', u'both', u'through', u'yourselves', 
        #                      u'its', u'before', u'o', u'hadn', u'herself', u'll', u'had', u'should', 
        #                      u'to', u'only', u'won', u'under', u'ours', u'has', u'do', u'them', u'his', 
        #                      u'very', u'they', u'not', u'during', u'now', u'him', u'nor', u'd', u'did', 
        #                      u'didn', u'this', u'she', u'each', u'further', u'where', u'few', u'because', 
        #                      u'doing', u'some', u'hasn', u'are', u'our', u'ourselves', u'out', u'what', u'for', 
        #                      u'while', u're', u'does', u'above', u'between', u'mustn', u't', u'be', u'we', 
        #                      u'who', u'were', u'here', u'shouldn', u'hers', u'by', u'on', u'about', u'couldn', 
        #                      u'of', u'against', u's', u'isn', u'or', u'own', u'into', u'yourself', u'down', 
        #                      u'mightn', u'wasn', u'your', u'from', u'her', u'their', u'aren', u'there', u'been', 
        #                      u'whom', u'too', u'wouldn', u'themselves', u'weren', u'was', u'until', u'more', 
        #                      u'himself', u'that', u'but', u'don', u'with', u'than', u'those', u'he', u'me', 
        #                      u'myself', u'ma', u'these', u'up', u'will', u'below', u'ain', u'can', u'theirs', 
        #                      u'my', u'and', u've', u'then', u'is', u'am', u'it', u'doesn', u'an', u'as', u'itself', 
        #                      u'at', u'have', u'in', u'any', u'if', u'again', u'no', u'when', u'same', u'how', 
        #                      u'other', u'which', u'you', u'shan', u'needn', u'haven', u'after', u'most', u'such', 
        #                      u'why', u'a', u'off', u'i', u'm', u'yours', u'so', u'y', u'the', u'having', u'once'])
    
    # purpose: split the line and capture the count for each ngram word
    # input: key(None), value(ngram\tcount\tpage\tbook)
    # output: nothing; counts are stored in the mapper's internal dict word_counts
    def mapper(self, _, line):
        ngram, count, page, book = line.lower().strip().split("\t")
        # drop word if it is an nltk stopword
        for word in ngram.split(' '):
            if word not in self.stopWords:
                self.word_counts[word] += int(count)
            
    # purpose: emit the word and count pairs to the combiner for summing counts for each word
    # input: use the mapper's internal word_counts dict
    # output: key(word), value(count for the word emitted from the mapper)
    def mapper_final(self):
        for word, count in self.word_counts.iteritems():
            yield word, count
    
    # purpose: sum up the counts for the same words processed by a mapper
    # input: key(word), value(count for the word emitted from the mapper)
    # output: key(word), value(sum of the word's counts)
    def combiner(self, word, count):
        yield word, sum(count)
    
    # purpose: sum up all of the word counts across the mappers
    # input: key(word), value(sum of the word's counts from the combiners)
    # output: key(sum of the word's counts), value(word)
    def reducer(self, word, count):
        #yield None, (int(sum(count)), word)
        yield sum(count), word
    
    # purpose: get the sorted outputs
    def reducer_sort(self, count, word):
        for w in word:
            yield count, w
    
    # END STUDENT CODE 5.4.1.B
        
if __name__ == '__main__':
    mostFrequentWords.run()