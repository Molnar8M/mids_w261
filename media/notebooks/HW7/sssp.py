#!/usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import os, sys, ast, operator

# purpose: MRJob class to traverse frontiers for shortest path graph distances
class MRsssp(MRJob): 
    
    # purpose: steps needed traverse frontiers
    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_init, 
                   mapper = self.mapper,
                   reducer_init = self.reducer_init,
                   reducer = self.reducer,
                   #reducer_final = self.reducer_final,
            )
        ]
    
    # set command line option to accept start and end node
    def configure_options(self):
        super(MRsssp, self).configure_options()
        self.add_passthrough_option('--start', default='1', type=str, help='start node')
        self.add_passthrough_option('--end', default='1', type=str, help='end node')
    
    # purpose: set the start node for the mappers
    def mapper_init(self):
        self.start = self.options.start

    # purpose: decompose the node input lines
    # input: line (node /t list of edge:weight pairs) or line (node \t edges | distance | path | state)
    # output: key (node), value ([edges, distance, path, state]))
    def mapper(self, _, line):
        # break apart the initial input line format
        key, info = line.strip().split('\t')
        
        # check for the frontier line format
        info = info.split('|') 
        
        # parse the initial or frontier input line
        if len(info) != 4: # initial line
            node = key
            edges = ast.literal_eval(info[0])
            distance = sys.maxint
            path = list()
            state = None
        else: # frontier line
            node = key
            edges = ast.literal_eval(info[0])
            distance = int(info[1])
            path = ast.literal_eval(info[2])
            state = info[3]
        
        # check for the start node without a state to change state and distance
        if node == self.start and state == None:
            distance = 0
            state = 'q'
            
        # yield the current node and the edges of nodes that are queued
        if state == 'q':
            yield node, (edges, distance, path, 'v')
            path.append(node)
            for n, weight in edges.iteritems():
                yield n, ({}, distance + weight, path, 'q')
        else:
            yield node, (edges, distance, path, state)
 
    # purpose: set the end node for the reducers
    def reducer_init(self):
        self.end = self.options.end
        self.path = []
        
    # purpose: aggregate the output from the mappers
    # input: key (node), values ([edges, distance, path, state])
    # output: key (node), values ([edges, distance, path, state])
    def reducer(self, key, values):
        # determine the node
        node = key

        # create lists for the value items
        edge_dict = dict()
        distance_list = list()
        path_list = list()
        state_list = list()
        
        # iterate through the generated value sets for the specified keys and aggregate items
        for v in values:
            #print node, v
            edges, distance, path, state = v
            
            # check if the node is visited and only keep those attributes by breaking out of the loop
            #if state == 'v':
            #    edge_dict = edges
            #    distance_list.append(distance)
            #    path_list = path
            #    state_list.append(state)
            #    break
            
            # collect info for aggregating for others nodes
            # keep the non-empty edge list for passing through
            #if edges: edge_dict = edges
            #distance_list.append(distance)
            #if path: path_list = path
            #state_list.append(state) 
            
            # experiment - collect all info concoctions in memory (not good)
            if edges: edge_dict = edges
            distance_list.append(distance)
            path_list.append(path)
            state_list.append(state)
            
        # experiment - decipher who to keep given min distance
        min_index, min_distance = min(enumerate(distance_list), key=operator.itemgetter(1))
        final_path = path_list[min_index]
        final_state = state_list[min_index]
        #print "node: ", node
        #print "edge_dict: ", edge_dict
        #print "ind, val: ", min_index, min_distance
        #print "path: ", final_path
        #print "state: ", final_state
        
        # experiment - check the state and act accordingly
        if final_state == 'v':
            self.increment_counter('sssp reducer', 'visted', 1)
            # check the exit criteria
            if node == self.end:
                final_state = 'E'
        elif final_state == 'E':
            self.increment_counter('sssp reducer', 'visted', 1)
        elif final_state == 'q': 
            self.increment_counter('sssp reducer', 'other', 1)
        else: 
            final_state = 'u'
            self.increment_counter('sssp reducer', 'other', 1) 
        
        yield node, (edge_dict, min_distance, final_path, final_state)
          
        # set the state given v, q, u priority
        #if 'v' in state_list: 
        #    s = 'v' 
        #    self.increment_counter('sssp reducer', 'visted', 1)
        #    # check the exit criteria
        #    if node == self.end:
        #        s = 'E'
            
            # capture the completed path if it is encountered
            #if node == self.end:
                ##self.increment_counter('group', 'visited', 0)
                #self.path = [node, edge_dict, min(distance_list), path_list, 'E']
                ##self.target_path = str(key) + "\t" + str(edges) + '|' + str(Dmin) + '|' + str(path) + '|' + "F"
        #elif 'q' in state_list: 
        #    s = 'q' 
        #    self.increment_counter('sssp reducer', 'other', 1)
        #else: 
        #    s = 'u'
        #    self.increment_counter('sssp reducer', 'other', 1)
            
        #yield node, (edge_dict, min(distance_list), path_list, s)
        
    # purpose: yield an additional record for the final path in addition to all else
    # yield: key (node), key (node), values ([edges, distance, path, state])
    def reducer_final(self):
        if self.path:
            node, edge_dict, min_distance, path_list, s = self.path
            yield node, (edge_dict, min_distance, path_list, s)

if __name__ == '__main__':
    MRsssp.run()