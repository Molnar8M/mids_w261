#!/usr/bin/python
from sssp import MRsssp
import sys
import ast
import time

def main(argv):
    if len(argv) != 4:
        print 'hw720_driver.py <graph file> <local|hadoop> <start word> <end word>'
        sys.exit(2)
        
    # ingest the indices for word lookup
    indices = {}
    for line in open('./indices.txt').readlines():
        word, count = line.strip().split("\t")
        indices[word] = count
        
    start_time = time.time()
    
    hw720_driver(argv[0], argv[1], indices[argv[2]], indices[argv[3]])
    
    print "\nsssp run time for %s (%s) to %s (%s): %.2f min" % (argv[2], indices[argv[2]], 
                                                                argv[3], indices[argv[3]],
                                                                (time.time() - start_time)/60)

def hw720_driver(input_file, mode, start, end):
    print "Processing %s using %s mode starting with %s and ending with %s." % (input_file,
                                                                              mode, start, end)
    
    # set up the mrjob
    mr_job = MRsssp(args=[input_file, '--start', start, '--end', end, '-q', '-r', 
                          mode, '--no-strict-protocol', '--cleanup-on-failure=ALL', 
                          '--cleanup=ALL'])

    prev_counters = {}
    shortest_path = None
    i = 0
    while (1):
        i += 1
        with mr_job.make_runner() as runner: 
            print "\niteration" + str(i) + ":"
            runner.run()
            f = open(input_file, 'w+')
            for line in runner.stream_output():
                node = line.split('\t')[0].strip('"')
                edges, distance, path, state = ast.literal_eval(line.split('\t')[1].strip('"'))
                
                # output formatted line to the file
                output = "%s\t%s|%s|%s|%s" % (str(node), str(edges), str(distance), str(path), str(state))
                #print output
                f.writelines(output + '\n')
                
                # check for the end state and output the shortest path
                if state == 'E':
                    path.append(node)
                    shortest_path = '-'.join(path)
                    min_distance = distance
                    #print line
 
            # output the counters
            counters = runner.counters()[0]['sssp reducer']  
            print "\ncounters:"
            for k, v in counters.iteritems():
                print "\t%s\t%s" % (k, v)
                
            # check if all nodes are visited or if the counters are not changing and exit the loop
            if 'other' not in counters.keys() or counters == prev_counters: 
                break
                
            # update the prev_counters
            prev_counters = counters

        f.close()
        
    # output the shortest path
    print "\nShortest path between %s and %s: %s with a distance of %s" % (start, 
                                                        end, shortest_path, min_distance)
    
if __name__ == "__main__":
    main(sys.argv[1:])