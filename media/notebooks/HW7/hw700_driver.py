#!/usr/bin/python
from sssp import MRsssp
import sys
import ast
import time

def main(argv):
    if len(argv) != 3:
        print 'hw700_driver.py <graph file> <start node> <end node>'
        sys.exit(2)
    start_time = time.time()
    
    hw700_driver(argv[0], argv[1], argv[2])
    
    print "\nsssp run time: %.2f sec" % (time.time() - start_time)

def hw700_driver(input_file, start, end):

    # set up the mrjob
    mr_job = MRsssp(args=[input_file, '--start', start, '--end', end, '-q', '-r', 'local', '--no-strict-protocol'])

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
                print output
                f.writelines(output + '\n')
                
                # check for the end state and output the shortest path
                if state == 'E':
                    path.append(node)
                    shortest_path = '-'.join(path)
                    min_distance = distance
                #else:
                #    f.writelines(output + '\n')
 
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
            
        #if i == 4: break

        f.close()
        
    # output the shortest path
    print "\nShortest path between %s and %s: %s with a distance of %s" % (start, 
                                                        end, shortest_path, min_distance)
    
if __name__ == "__main__":
    main(sys.argv[1:])