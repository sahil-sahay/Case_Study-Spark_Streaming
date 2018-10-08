import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level,Logger}

//Singleton object SparkFileStreamingWordCount
object SparkFileStreamingWordCount {

//The execution entry point for the spark streaming application
  def main(args: Array[String]): Unit = {
    println("hey Spark Streaming")

    //Setting up spark configurations with 2 cores.
	val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSteamingExample")
    
	//Inintializing a new spark context based on above configurations
	val sc = new SparkContext(conf)
    
	//Initializing logger in spark
	val rootLogger =Logger.getRootLogger()
	
	//Setting log info to error
    rootLogger.setLevel(Level.ERROR)
	
	//Creating a new spark streaming context that would be triggered every 15 seconds
    val ssc = new StreamingContext(sc, Seconds(15))
	
    //Streaming from a text file that is passed to the appllication as an argument during runtime.
	val lines = ssc.textFileStream(args(0))
	
	//splitting each line of file based upon a space & then flattening it into respective words.
    val words = lines.flatMap(_.split(" "))
	
	//creating another rdd from the above words & assigning digit 1 for each word within the RDD. Counting occurence of each word
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
	
	//printing the count of each word withi the file
    wordCounts.print()
	
	//Starting the spark streaming context
    ssc.start()
	
	//Handler for termination of streaming context
    ssc.awaitTermination()
  }
}