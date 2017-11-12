#!/usr/bin/python
from numpy import random,array
import sys, time
from weightedOLS import MrJobWeightedOLS

start = time.time()

learning_rate = 0.005
stop_criteria = 0.0000005

# Generate random values as inital weights
weights = array([random.uniform(-3,3),random.uniform(-3,3)])
# Write the weights to the files
with open('weights.txt', 'w+') as f:
    f.writelines(','.join(str(j) for j in weights))

# create a mrjob instance for batch gradient descent update over all data
mr_job = MrJobWeightedOLS(args=['hw65.csv','--file=weights.txt'])

# Update centroids iteratively
i = 0
while(1):
    # print and output the weights
    iter_line = 'iteration = %s\tweights = %s' % (str(i), weights)
    print iter_line
    
    # Save weights from previous iteration
    weights_old = weights
    
    with mr_job.make_runner() as runner: 
        runner.run()
        # stream_output: get access of the output 
        for line in runner.stream_output():
            # value is the gradient value
            key,value =  mr_job.parse_output_line(line)
            # Update weights
            weights = weights + learning_rate*array(value)
    i = i + 1
    
    # Write the updated weights to file 
    with open('weights.txt', 'w+') as f:
        f.writelines(','.join(str(j) for j in weights))
        
    # Stop if weights get converged
    if(sum((weights_old - weights)**2) < stop_criteria):
        break

final_weights = weights
print "\nFinal weights: ", final_weights
print '\nTime to completion: %.2f min' % ((time.time() - start)/60)