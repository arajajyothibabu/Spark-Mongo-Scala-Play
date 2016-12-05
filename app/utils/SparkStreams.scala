package utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * The spark streams code example adapted from: http://spark.apache.org/docs/latest/streaming-programming-guide.html
  * Created by jyothi on 3/12/16.
  */

object SparkStreams extends MongoWithSparkContext {

  /**
    * See: http://spark.apache.org/docs/latest/streaming-programming-guide.html
    * Requires: Netcat running on localhost:9999 using command `nc -lk 9999`
    *
    * @param args takes an optional single argument for the connection string
    * @throws Throwable if an operation fails
    */
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args)

    import com.mongodb.spark.sql._
    import org.apache.spark.streaming._

    val ssc = new StreamingContext(sc, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999) //keep writing in the terminal

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreachRDD({ rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val wordCounts = rdd.map({ case (word: String, count: Int) => WordCount(word, count) }).toDF()
      wordCounts.show()
      import com.mongodb.spark.config.WriteConfig
      val writeConfig  = WriteConfig(Map("collection" -> "streams", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
      wordCounts.write.mode("append").mongo(writeConfig)
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }

  case class WordCount(word: String, count: Int)

  /** Lazily instantiated singleton instance of SQLContext */
  object SQLContextSingleton {

    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (Option(instance).isEmpty) {
        instance = SQLContext.getOrCreate(sparkContext)
      }
      instance
    }
  }

}