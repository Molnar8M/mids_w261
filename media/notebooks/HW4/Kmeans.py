#!/usr/bin/env python
import numpy as np
from numpy import argmin, array, random
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import chain
from operator import add
from collections import defaultdict
import os, re

def startCentroidsA(k):
    points = random.uniform(size=[k, 1000])
    sum_points = np.sum(points, axis = 1)
    centroids = np.true_divide(points.T, sum_points).T
    return centroids
    
def startCentroidsBC(k):
    counter = 0
    for line in open("topUsers_Apr-Jul_2014_1000-words_summaries.txt").readlines():
        if counter == 1:        
            data = re.split(",",line)
            globalAggregate = [float(data[i+3])/float(data[2]) for i in range(1000)]            
        counter += 1
    #perturb the global aggregate for the k initializations    
    centroids = []
    for i in range(k):
        rndpoints = random.sample(1000)
        peturpoints = [rndpoints[n]/10+globalAggregate[n] for n in range(1000)]
        centroids.append(peturpoints)
        total = 0
        for j in range(len(centroids[i])):
            total += centroids[i][j]
        for j in range(len(centroids[i])):
            centroids[i][j] = centroids[i][j]/total
    return centroids

def startCentroidsD(k):
    counter = 0
    centroids = []
    for line in open("topUsers_Apr-Jul_2014_1000-words_summaries.txt").readlines():
        if counter >= 2:        
            data = re.split(",",line)
            # normalize the class aggregates
            centroids.append([float(data[i+3])/float(data[2]) for i in range(1000)])
        counter += 1
    return centroids

# purpose: calculate find the nearest centroid for data point 
def get_nearest_centroid(data_point, centroid_points):
    # list to numpy arrays
    centroids = np.array(centroid_points)
    data = np.array(data_point)  
    # Calculate the Euclidean distance between the data_point and each of the centroids
    distances = np.sqrt(np.sum(np.square(np.subtract(data, centroids)), axis=1))  
    # Return the index of the nearest centroid for this data point
    return np.argmin(distances)

# purpose: check whether centroids converge
def stop_criterion(centroid_points_old, centroid_points_new, T):
    # lists to numpy arrays
    old_centroids = np.array(centroid_points_old)
    new_centroids = np.array(centroid_points_new)
    diffs = np.absolute(np.subtract(old_centroids, new_centroids))
    if np.amax(diffs) < T:
        flag = True
        print 'Stopping threshold reached (%.3f): %.3f' %(T, np.amax(diffs))
    else:
        flag = False
    return flag

class MRKmeans(MRJob):
    
    centroid_points = []
    k = 4    
    
    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_init, 
                   mapper=self.mapper,
                   combiner = self.combiner,
                   reducer=self.reducer
            )
        ]
    
    # purpose: load centroids info from file
    def mapper_init(self):
        print "current path:", os.path.dirname(os.path.realpath(__file__))
        self.centroid_points = [map(float,s.split('\n')[0].split(',')) for s in open("centroids.txt").readlines()]        
        #print "centroids: ", self.centroid_points
        
    # purpose: load data and output the nearest centroid index and data point 
    # input: lines from data file
    # output: key(nearest centroid index), value(local word freq list, userid, code, count) 
    def mapper(self, _, line):
        # split out all of the fields
        D = (map(float, line.split(',')))
        # peel off the non-features
        userid = D[0]
        code = int(D[1])
        total = int(D[2])
        # calculate the frequency of each word in the record
        D_wfs = [float(x) / total for x in D[3:]]
        # output the nearest centroid index, word freq list, userid, code, and count
        yield int(get_nearest_centroid(D_wfs, self.centroid_points)), (D_wfs, userid, code, 1)
        
    # purpose: combine sum of data points, that are partitioned by centroid idx, locally
    # input: key(nearest centroid index), value(local word freq lists, userids codes, and counts)
    # output: key(nearest centroid index), value(list of summed word freqs, dict of code counts)
    def combiner(self, idx, inputdata):
        # initialize list and dict for combining
        sum_wordfreqs = [0] * 1000
        codecounts = defaultdict(int)
        # sum features and code counts for all records in the partition
        for wordfreqs, userid, code, count in inputdata:
            # element-wise list sum of word frequencies
            sum_wordfreqs = list(map(add, sum_wordfreqs, wordfreqs))
            # preserve the number of code instances within the partition
            codecounts[code] += int(count)
        # output the nearest centroid index, and combined word freqs and code counts
        yield idx, (sum_wordfreqs, codecounts)
        
    # purpose: aggregate sum for each cluster and then calculate the new centroids
    # input: key(nearest centroid index), value(list of summed word freqs, dict of code counts)
    # output: 
    def reducer(self, idx, inputdata):
        # initialize list and dict for combining
        sum_wordfreqs = [0] * 1000
        codecounts = defaultdict(int)
        # sum features and code counts for all records in the partition
        for wordfreqs, counts in inputdata:
            # element-wise list sum of word frequencies
            sum_wordfreqs = list(map(add, sum_wordfreqs, wordfreqs))
            
            # preserve the number of code instances within the partition
            for code, count in counts.iteritems():
                codecounts[code] += int(count)
                
        # adjust the centroids
        codesum = sum(codecounts.values())
        final_wordfreqs = [x / codesum for x in sum_wordfreqs]
            
        # output the centroid index, and summed word freqs and code counts
        yield idx, (final_wordfreqs, codecounts)

if __name__ == '__main__':
    MRKmeans.run()