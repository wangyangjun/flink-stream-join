## flink-stream-join
[Apache Flink Streaming](https://ci.apache.org/projects/flink/flink-docs-release-0.10/apis/streaming_guide.html#windows) only 
supports join(join two streams) operator on the same window which could not some requirement in some real business needs.  
#### Case -- Advertisements click analysising:
An advertisement service provider generates a realtime(every min) bill of a customer base on how many legal clicks of the customer's advertisments.
A click is legal if it is clicked in one certain time(20 mins) after shown.  
There are two streams in this case. 
- Advertisement strem, a stream of shown advertisements which could be described as Tuple(Shown time, Advertisement ID)
- Click stream, a stream of click event - Tuple(Clicked time, Advertisement ID)  

**Question:** How to get a new stream of legal clicks?  
TumblingTimeWindows on joined stream will lose data. SlidingTimeWindows on joined stream would generate duplicate data and it is 
also possible to lose data. Please [see](http://stackoverflow.com/questions/33849462/how-to-avoid-repeated-tuples-in-flink-slide-window-join).  


##### Solution 1:  
Let flink support join two streams on separate windows like Spark streaming. In this case, implement SlidingTimeWindows(21 mins, 1 min) on **advertisement stream** and TupblingTimeWindows(1 min) on **Click stream**, then join these two windowed streams.      
- TupblingTimeWindows could avoid duplicate records in the joined stream.  
- 21 mins size SlidingTimeWindows could avoid missing legal clicks.   

One issue is there would be some illegal click(click after 20 mins) in the joined stream. This problem could be fixed easily by adding a filter.  

##### Solution 2:  
Flink supports join operation without window.  A join operator implement the interface [TwoInputStreamOperator](https://ci.apache.org/projects/flink/flink-docs-release-0.10/api/java/org/apache/flink/streaming/api/operators/class-use/TwoInputStreamOperator.html) keeps two buffers(time length based) of these two streams and output one joined stream.   
