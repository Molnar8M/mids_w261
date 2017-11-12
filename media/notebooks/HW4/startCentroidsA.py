
from random import randint
import linecache
import numpy
###################################################################################
##Part A of this question - Shankar's function
###################################################################################
def centroidsA(k):

   # Use a list to store the k centroid arrays
    centroids = []

    numLines = []
    for i in range (0,k):
        # Generate K random numbers so that data can be pulled
        # from those rows in the topUsers_Apr-Jul_2014_1000-words.txt file
        num = randint(0, 1000)
        numLines.append(num)
        
    print numLines
   
    line = []
    # Pull the lk lines from the file
    for linenum in numLines:
        entry = linecache.getline("topUsers_Apr-Jul_2014_1000-words.txt", linenum)
        data = entry.strip("\n").split(",")
       
        # Normalize the data by dividing every entry (after field 3) by the total (field 2)
        lineAggregate = [float(data[i+3])/float(data[2]) for i in range(1000)]
        centroids.append(lineAggregate)
    # return the k centroid arrays
    return centroids

if __name__ == '__main__':
    centroids = centroidsA(4)
    print numpy.shape(centroids)