package utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * The spark streams code example adapted from: http://spark.apache.org/docs/latest/streaming-programming-guide.html
  * Created by jyothi on 3/12/16.
  */

object SparkStreams extends MongoWithSparkContext {

  /**
    * Run this main method to see the output of this quick example or copy the code into the spark shell
    *
    * See: http://spark.apache.org/docs/latest/streaming-programming-guide.html
    * Requires: Netcat running on localhost:9999
    *
    * @param args takes an optional single argument for the connection string
    * @throws Throwable if an operation fails
    */
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args) // Don't copy and paste as its already configured in the shell

    import com.mongodb.spark.sql._
    import org.apache.spark.streaming._

    val ssc = new StreamingContext(sc, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreachRDD({ rdd =>
      val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext)
      import sparkSession.implicits._

      val wordCounts = rdd.map({ case (word: String, count: Int) => WordCount(word, count) }).toDF()
      wordCounts.write.mode("append").mongo()
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }

  case class WordCount(word: String, count: Int)

  /** Lazily instantiated singleton instance of SQLContext */
  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _

    def getInstance(sparkContext: SparkContext): SparkSession = {
      if (Option(instance).isEmpty) {
        instance = SparkSession.builder().getOrCreate()
      }
      instance
    }
  }

}
