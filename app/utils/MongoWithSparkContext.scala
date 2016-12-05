package utils

import org.apache.spark.{SparkConf, SparkContext}

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig

/**
  * Created by jyothi on 3/12/16.
  */
private[utils] trait MongoWithSparkContext {

  def getSparkContext(args: Array[String]): SparkContext = {
    val uri: String = args.headOption.getOrElse("mongodb://localhost/jyothi.companies") //DB: jyothi, collection: companies
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkMongoScalaPlay")
      .set("spark.app.id", "SparkMongoScalaPlay")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val sc = new SparkContext(conf)
    MongoConnector(sc).withDatabaseDo(WriteConfig(sc), {db => /*db.drop()//drops the entire db*/})
    sc
  }

}
