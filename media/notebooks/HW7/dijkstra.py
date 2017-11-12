#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.emr import EMRJobRunner
from boto.s3.key import Key
import ast
import sys
import boto

class MRDijkstra(MRJob):

    # generator that returns boto.s3.Key names.
    def write_to_s3(bucket, key, string):
        emr = EMRJobRunner()
        c = emr.fs.make_s3_conn()
        b = c.get_bucket(bucket)
        k = Key(b)
        k.key = key
        k.set_contents_from_string(string)
    
    # set command line option to accept start and end node
    def configure_options(self):
        super(MRDijkstra, self).configure_options()

        self.add_passthrough_option('--startnode', default='1', type=str, help='starting node for single source shortest path')
        self.add_passthrough_option('--endnode', default=None, type=str, help='target node to be visited')
        self.add_passthrough_option('--bucket', default=None, type=str, help='bucket to write secondary output')
        self.add_passthrough_option('--runmode', default=None, type=str, help='local or emr')

    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_init, 
                   mapper = self.mapper,
                   reducer_init = self.reducer_init,
                   reducer = self.reducer,
                   reducer_final = self.reducer_final,
                   
            )
        ]
    
    # set the startnode
    def mapper_init(self):
        self.startnode = self.options.startnode
        #print self.startnode
    
    #def mapper(self, _, line):

    # emit (node, [edges, distance, path, state]))
    def mapper(self, _, line):
        # parse input
        line = line.strip()
        data = line.split('\t')
        values = data[1].strip('"').split("|")
        
        #print line
        #print data
        #print values
        #print len(values)
        
        # init variables
        node = None
        edges = None
        path = list()
        dist = sys.maxint
        state = None
        
        # parse the input for first iteration
        if len(values) >= 1:
            node = data[0].strip('"')
            edges = ast.literal_eval(values[0])
        # parse the input for subsequent iterations 
        if len(values) == 4:
            dist = int(values[1])
            path = ast.literal_eval(values[2])
            state = values[3]
        
        # for first pass set start node to q status
        if state == None and self.startnode == node:
            state = 'q'
            dist = 0
        
        # for frontier nodes emit node with its neighbors
        if state == 'q':
            yield node, (edges, dist, path, 'v')
            path.append(node)

            for n in edges.iterkeys():
                yield n, (None, dist + 1, path, 'q')
        # else emit the current node
        else:
            yield node, (edges, dist, path, state)

    # set the end node 
    def reducer_init(self):
        self.endnode = self.options.endnode
        self.target_reached = -1
        self.target_path = ""
        self.s3bucket = self.options.bucket
        self.runmode = self.options.runmode
        #print self.endnode
        #print self.target_reached
        #print self.target_path

    # purpose: combine the output from the mappers
    # yield: key, value(edge list, min distance, path, state)
    def reducer(self, key, values):
        edges = {}
        dist = list()
        state = list()
        path = list()
        f_state = ''
        
        #print key

        for v in values:
            #print v
            v_edges = v[0]
            v_dist = v[1]
            v_path = v[2]
            v_state = v[3]
            
            if v_state == 'v':
                edges = v_edges
                dist.append(v_dist)
                path = v_path
                state.append(v_state)
                break
            
            dist.append(v_dist)
            state.append(v_state)

            if v_edges != None:
                edges = v_edges
            if v_path != None and len(v_path) > 0:
                path = v_path
                
        Dmin = min(dist)
        
        if 'v' in state:
            f_state = 'v'
            if self.endnode != None and key.strip('"') == self.endnode:
                self.increment_counter('group', 'visited', 0)
                self.target_reached = 1
                self.target_path = str(key) + "\t" + str(edges) + '|' + str(Dmin) + '|' + str(path) + '|' + "F"
        elif 'q' in state:
            f_state = 'q'
            self.increment_counter('group', 'pending', 1)
        else:
            f_state = 'u'
            self.increment_counter('group', 'pending', 1)
            
        yield key, str(edges) + '|' + str(Dmin) + '|' + str(path) + '|' + f_state

    # purpose: determine if the target was reached and yield the target path
    # yield: target key (node), value (edges | min distance | path | state)
    def reducer_final(self):
        #print self.target_reached
        if self.target_reached == 1:
            sys.stderr.write('Target reached')
            if self.runmode == 'emr':
                sys.stderr.write(self.target_path)
                s3_key = 'hw7/visited.txt'
                emr = EMRJobRunner()
                c = emr.fs.make_s3_conn()
                b = c.get_bucket(self.s3bucket)
                k = Key(b)
                k.key = s3_key
                k.set_contents_from_string(self.target_path)
                #self.write_to_s3(self.options.bucket, s3_key, self.target_path)
            else:
                yield self.target_path.split('\t')[0], self.target_path.split('\t')[1]
            
        
if __name__ == '__main__':
    MRDijkstra.run()