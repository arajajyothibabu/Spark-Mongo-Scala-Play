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
    import org.bson.Document

    //Reading data from MongoDb as RDD
    val companiesRdd = MongoSpark.load(sc) //we can also read with custom readConfig
    println("Total Companies:: " + companiesRdd.count())
    println("One Company:: " + companiesRdd.first.toJson)

    import com.mongodb.spark.config._

    // Filtering an rdd in Spark
    val filteredRdd = companiesRdd.filter(doc => doc.getInteger("number_of_employees") == 500) //companies with more than 500
    println("Filtered Companies with more than 500 employees:: " + filteredRdd.count)
    println("Filtered One Company:: " + filteredRdd.first.toJson)

    // Filtering an rdd using an aggregation pipeline before passing to Spark
    val aggregatedRdd = companiesRdd.withPipeline(Seq(Document.parse("{ $match: { competitions: { $eq : [] } } }")))
    println("Aggregation Companies with no competitors:: " + aggregatedRdd.count)
    println("Aggregation One Company:: " + aggregatedRdd.first.toJson)

    // Saving data with a custom WriteConfig
    val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))

    val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))
    MongoSpark.save(sparkDocuments, writeConfig)

    // Loading and analyzing data from MongoDB with a custom ReadConfig
    val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)
    println("Custom Count:: " + customRdd.count)
    println("Custom One Document:: " + customRdd.first.toJson)

  }

}
