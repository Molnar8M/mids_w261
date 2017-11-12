#!/usr/bin/env python
#START STUDENT CODE43
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostFrequentVisits(MRJob):
    
    def mapper_get_pages(self, _, line):
        (v, pageid, count, c, visitorid) = line.split(',')
        yield (pageid, int(count))

    def combiner_count_pages( self, page, counts):
        # sum the page counts
        yield (page, sum(counts))

    def reducer_count_pages(self, page, counts):
        # send all (num_occurrences, page) pairs to the same reducer.
        yield None, (sum(counts), page)
        
    def reducer_final_sort_clip(self, _, page_count_pairs):
        for (count, page) in sorted(page_count_pairs, reverse=True)[0:5]:
            yield (page, count)
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_pages,
                   combiner=self.combiner_count_pages,
                   reducer=self.reducer_count_pages),
            MRStep(reducer=self.reducer_final_sort_clip)
        ]

if __name__ == '__main__':
    MRMostFrequentVisits.run()

#END STUDENT CODE43