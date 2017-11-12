#!/usr/bin/env python
#START STUDENT CODE44
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
#from mrjob.compat import get_jobconf_value

# input: V, PageID, 1, C, Visitor && PageID, URL
# output (most freq visitor per page): webpageID webpage URL, visitor ID, max count

class MRMostFrequentVisitors(MRJob):
    
    def init_pages(self):
        self.pages = {}

    def mapper_leftjoin(self, _, line):
        l = line.split(',')
        if len(l) == 5:
            yield l[1], ("lefttable", l[4], l[2])
        else:
            yield l[0], ("righttable", l[1])
            
    def reducer_leftjoin(self, key, values):
        hits = list()
        urls = list()

        for val in values:
            if val[0] == u'lefttable':
                hits.append(val)
            else:
                urls.append(val)
        
        for h in hits:
            # output: pageid, (URL,visitorID,count)
            if len(urls)==0:
                yield key, (h[1], h[2], None)
            for u in urls:
                yield key, (h[1], h[2], u[1])
    
    def mapper_freqvis(self, key, values):
        yield (int(key),values[0],values[2]), int(values[1])
            
    def reducer_freqvis(self, key, value):
        yield key[0], (sum(value), key[1], key[2])
        
    def reducer_maxvis(self, key, values):   
        yield key, (max(values))
           
    def steps(self):
        return [
            # leftjoin to add the URL to the pageid/visitorid/1 record
            MRStep(mapper_init=self.init_pages,
                   mapper=self.mapper_leftjoin,
                   reducer=self.reducer_leftjoin),
            # sum up the number of counts for each pageid/visitorid/URL key
            MRStep(mapper=self.mapper_freqvis,
                   reducer=self.reducer_freqvis),
            # group the output by pageid/URL to find max visitorcount/visitorid then emit
            MRStep(reducer=self.reducer_maxvis),
        ]

if __name__ == '__main__':
    MRMostFrequentVisitors.run()

#END STUDENT CODE44