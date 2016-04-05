from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.lines as mlines


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")
    global pwords,nwords
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    positive=[]
    negative=[]
    t=[]
    for element in counts:

            if element != []:
        
                positive.append(element[0][1])
        
                negative.append(element[1][1])

    plt.plot(positive,'.b-',negative,'.g-')
    plt.ylabel('Word Count')
    plt.xlabel('Time Stamp')

    positive_line = mlines.Line2D([], [], color='blue', marker='.',
                          markersize=12, label='positive')
    
    

    
    negative_line = mlines.Line2D([], [], color='green', marker='.',
                          markersize=12, label='negative')
    
    plt.legend(handles=[positive_line,negative_line])

    plt.show()



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    f=open(filename)
    l=[]
    for word in f.read().split():
        l.append(word)

    return l


def mapping(word):

    if word in pwords:
        return ("positive",1)
    elif word in nwords:
        return ("negative",1)
    else:
        return (word,1)

def updateFunction(newValues, runningCount):
    
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount) 








def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    

    words = tweets.flatMap(lambda line: line.split(" "))
    pairs = words.map(mapping).filter(lambda x: x[0]=='positive' or x[0]=='negative')
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)
    runningCount=wordCounts.updateStateByKey(updateFunction)
    runningCount.pprint()

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    wordCounts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
