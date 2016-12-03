package utils

/**
  * Created by jyothi on 3/12/16.
  */

object MongoUtility extends MongoWithSparkContext {

  /**
    * Run this main method to see the output of this quick example or copy the code into the spark shell
    *
    * @param args takes an optional single argument for the connection string
    * @throws Throwable if an operation fails
    */
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args) // Don't copy and paste as its already configured in the shell

    import com.mongodb.spark._

    // Saving data from an RDD to MongoDB
    import org.bson.Document
    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    MongoSpark.save(documents)

    // Saving data with a custom WriteConfig
    import com.mongodb.spark.config._
    val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))

    val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))
    MongoSpark.save(sparkDocuments, writeConfig)

    // Loading and analyzing data from MongoDB
    val rdd = MongoSpark.load(sc)
    println(rdd.count)
    println(rdd.first.toJson)

    // Loading data with a custom ReadConfig
    val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)
    println(customRdd.count)
    println(customRdd.first.toJson)

    // Filtering an rdd in Spark
    val filteredRdd = rdd.filter(doc => doc.getInteger("test") > 5)
    println(filteredRdd.count)
    println(filteredRdd.first.toJson)

    // Filtering an rdd using an aggregation pipeline before passing to Spark
    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{ $match: { test : { $gt : 5 } } }")))
    println(aggregatedRdd.count)
    println(aggregatedRdd.first.toJson)
  }

}
