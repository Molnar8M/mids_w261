from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

# purpose: count number of nodes and links
class MRcounts(MRJob):
    
    # purpose: steps to count nodes and links
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer,
            )
        ] 
    
    # purpose: emit the number of links for each node
    # input: (node \t stripe of node:weight pairs)
    # output: key (None), value (number of links)
    def mapper(self, _, line):
        node, info = line.strip().split('\t')
        edges = ast.literal_eval(info)
        count = len(edges.items())
        yield None, count
        
    # purpose: sum the link counts
    # input: key (None), value (number of links)
    # output: key (number of nodes), value (total number of links)
    def reducer(self, _, counts):
        cnts = list(counts)
        yield len(cnts), (sum(cnts), float(sum(cnts))/len(cnts))

if __name__ == '__main__':
    MRcounts.run()