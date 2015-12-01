## flink-stream-join
[Apache Flink Streaming](https://ci.apache.org/projects/flink/flink-docs-release-0.10/apis/streaming_guide.html#windows) only 
supports join(join two streams) operator on the same window which could not some requirement in some real business needs.  
#### Case -- Advertisements click analysising:
An advertisement service provider generates the bill of a customer base on how many legal clicks of the customer's advertisments.
A click is legal if it is clicked in one certain time(10 mins) after shown.  
There are two streams in this case. 
- Shown advertisements, a stream of Tuple(Shown time, Advertisement ID)
- Click stream, a stream of Tuple(Clicked time, Advertisement ID)  

**Question:** How to get a new stream of legal clicks?  
TumblingTimeWindows on joined stream will lose data. SlidingTimeWindows on joined stream would generate duplicate data and it is 
also possible to lose data. Please [see](http://stackoverflow.com/questions/33849462/how-to-avoid-repeated-tuples-in-flink-slide-window-join).  
