from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

# purpose: emit degree frequencies
class MRdistribution(MRJob):

    # purpose: steps to emit degree frequencies
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer,
            )
        ] 
    
    # purpose: emit the link count frequency
    # input: (node \t stripe of node:weight pairs)
    # output: key (number of links), value (1)
    def mapper(self, _, line):
        node, info = line.strip().split('\t')
        edges = ast.literal_eval(info)
        count = len(edges.items())
        yield int(count), 1
        
    # purpose: sum the link count frequency
    # input: key (number of links), value (1)
    # output: key (number of links), value (frequency)
    def reducer(self, count, values):
        yield int(count), sum(values)

if __name__ == '__main__':
    MRdistribution.run()