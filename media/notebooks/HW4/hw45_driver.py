#!/usr/bin/env python
import numpy as np
import sys, re
from numpy import random
from Kmeans import MRKmeans, stop_criterion, startCentroidsA, startCentroidsBC, startCentroidsD

np.random.seed(0)

def main(argv):
    if len(argv) != 3:
        print 'hw45_driver.py <threshold> <k> <\'A\',\'B\',\'C\',\'D\'>'
        sys.exit(2)
    kmeans_driver(float(argv[0]), int(argv[1]), argv[2])

def kmeans_driver(threshold, k, init):
    # set up the job args
    mr_job = MRKmeans(args=['topUsers_Apr-Jul_2014_1000-words.txt', '--file=centroids.txt'])

    # initialize the centroids
    centroid_points = []
    #k = 4
    if init == 'A':
        centroid_points = startCentroidsA(k)
        print "(A) K=4 uniform random centroid-distributions over the 1000 words (generate 1000 random numbers and normalize the vectors)\n"
    elif init == 'B' or init == 'C':
        centroid_points = startCentroidsBC(k)
        print "(C) K=4 perturbation-centroids, randomly perturbed from the aggregated (user-wide) distribution\n"
    else:
        centroid_points = startCentroidsD(k)
        print "(D) K=4 \"trained\" centroids, determined by the sums across the classes\n"
        
    # write centroids to the expected file
    with open('centroids.txt', 'w+') as f:
        f.writelines(','.join(str(j) for j in i) + '\n' for i in centroid_points)
    f.close()    

    # update centroids iteratively
    i = 0
    code_clusters = [{}] * k
    while(1):
        # save previous centoids to check convergency
        centroid_points_old = centroid_points[:]
        print "iteration" + str(i) + ":"
        with mr_job.make_runner() as runner: 
            runner.run()
            # stream_output: get access of the output 
            for line in runner.stream_output():
                key, values =  mr_job.parse_output_line(line)
                #print key, values
                centroid = values[0]
                codes = values[1]
                centroid_points[key] = centroid
                code_clusters[key] = codes

        # Update the centroids for the next iteration
        with open('centroids.txt', 'w') as f:
            f.writelines(','.join(str(j) for j in i) + '\n' for i in centroid_points)

        print "\n"
        i = i + 1
        if(stop_criterion(centroid_points_old, centroid_points, threshold)):
            break

    print "\nTotal iterations:", i

    max_vals = []
    total_vals = []
    print ('\n%s\t%s\t\t%s\t\t%s\t\t%s\t\t%s') % ('cluster', 'human', 'cyborg', 'robot', 'spammer', 'total')
    print '============================================================================='
    for idx, cluster in enumerate(code_clusters):
        zero_val = one_val = two_val = three_val = 0
        total = float(sum(cluster.values()))
        if '0' in cluster.keys(): zero_val = cluster['0'] 
        if '1' in cluster.keys(): one_val = cluster['1'] 
        if '2' in cluster.keys(): two_val = cluster['2'] 
        if '3' in cluster.keys(): three_val = cluster['3'] 

        print ('%d\t%d (%.2f%%)\t%d (%.2f%%)\t%d (%.2f%%)\t%d (%.2f%%)\t%d') % (idx, zero_val, 
                (zero_val / total * 100), one_val, (one_val / total * 100), two_val, 
                (two_val / total * 100), three_val, (three_val / total * 100), total)

        #purity = sum of the max points for each cluster divided by sum of total points in each cluster
        max_vals.append(max(cluster.values()))
        total_vals.append(sum(cluster.values()))

    purity = float(sum(max_vals))/(sum(total_vals))
    print "purity = %.2f%%" % (100 * purity)

if __name__ == "__main__":
    main(sys.argv[1:])