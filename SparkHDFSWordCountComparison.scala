import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source._
import org.apache.log4j.{Level,Logger}


//Singleton object SparkHDFSWordCountComparison
object SparkHDFSWordCountComparison {

  //File path on local system
  private var localFilePath: File = new File("/home/acadgild/Documents/s26/usecase/inputs/test.txt")
  
  //File path on local HDFS
  private var dfsDirPath: String = "hdfs://localhost:8020/user/streaming"
  
  //Declaring variable to check for number of arguments passed to application during sun time.
  private val NPARAMS = 2


  //The execution entry point for the spark streaming application
  def main(args: Array[String]): Unit = {
    //parseArgs(args)
    println("SparkHDFSWordCountComparison : Main Called Successfully")

    println("Performing local word count")
	
	//Calling readFile function to read the local file
    val fileContents = readFile(localFilePath.toString())

    println("Performing local word count - File Content ->>"+fileContents)
    
	//Calling runLocalWordCount function to perform wordcount on the local file
	val localWordCount = runLocalWordCount(fileContents)

	
    println("SparkHDFSWordCountComparison : Main Called Successfully -> Local Word Count is ->>"+localWordCount)

    println("Performing local word count Completed !!")

    println("Creating Spark Context")

	//Setting up spark configurations with 2 cores.
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkHDFSWordCountComparisonApp")
    
	//Inintializing a new spark context based on above configurations
	val sc = new SparkContext(conf)
	
	//Initializing logger in spark
	val rootLogger =Logger.getRootLogger()
	
	//Setting log info to error
    rootLogger.setLevel(Level.ERROR)

    
    println("Spark Context Created")

    println("Writing local file to DFS")
	
	//HDFS path on which file from local would be copied before performing wordcount
    val dfsFilename = dfsDirPath + "/dfs_read_write_test"
	
	//Creating RDD from textFile
    val fileRDD = sc.parallelize(fileContents)
	
	//Saving the converted RDD onto HDFS
    fileRDD.saveAsTextFile(dfsFilename)
    println("Writing local file to DFS Completed")

    println("Reading file from DFS and running Word Count")
	
	//Reading file from HDFS & converting it into RDD
    val readFileRDD = sc.textFile(dfsFilename)

	//function to count total number of words from file placed on HDFS
    val dfsWordCount = readFileRDD
      .flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .map(w => (w, 1))
      .countByKey()
      .values
      .sum

	//Stopping the spark Context
    sc.stop()

	//Comparing the total number of words from each local & HDFS thereby displaying count & respective message
    if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) agree.")
    } else {
      println(s"Failure! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) disagree.")
    }



  }

  //Function to check number of arguments passed to the program during runtime.
  private def parseArgs(args: Array[String]): Unit = {
    //checking if lentgth of args is not equal to 2 , including the name of application itself if run from shell
	if (args.length != NPARAMS) {
      printUsage()
      System.exit(1)
    }
  }

  //Function to instruct the proper run sequence for this application with 2 number of arguments
  private def printUsage(): Unit = {
    val usage: String = "DFS Read-Write Test\n" +
      "\n" +
      "Usage: localFile dfsDir\n" +
      "\n" +
      "localFile - (string) local file to use in test\n" +
      "dfsDir - (string) DFS directory for read/write tests\n"

    println(usage)
  }

  //funtion to iterate through the entire file & return a list of each line within the file
  private def readFile(filename: String): List[String] = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }

  //Counting the total number of words that within the file on the local file system.
  def runLocalWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }
}