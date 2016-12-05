package utils

/**
  * Created by jyothi on 3/12/16.
  */

import org.apache.spark.sql.SQLContext
import org.bson.Document
import org.bson.types.ObjectId
import com.mongodb.spark.config._
import org.apache.spark.SparkContext


object SparkSQL extends MongoWithSparkContext {

  /**
    * @param args takes an optional single argument for the connection string
    * @throws Throwable if an operation fails
    */
  def main(args: Array[String]): Unit = {

    val sc = getSparkContext(args)

    import com.mongodb.spark._
    // Create SQLContext
    val sqlContext = SQLContext.getOrCreate(sc)

    // Import the SQL helper
    val df = MongoSpark.load(sqlContext)
    df.printSchema() //prints the document schema

    // Companies founded after year 2000
    //df.filter(df("founded_year") > 2000).show() //FIXME: filtering will be possible if no conflicting types for values

    //registering table for companies
    val companies = MongoSpark.load[SparkSQL.Company](sqlContext)
    companies.registerTempTable("companies")

    //loading small companies
    val smallCompanies = sqlContext.sql("SELECT name, founded_year FROM companies WHERE number_of_employees <= 50")
    smallCompanies.show()

    // Save the smallCompanies
    MongoSpark.save(smallCompanies.write.option("collection", "startUps"))
    println("Reading from the 'startUps' collection:")
    MongoSpark.load[SparkSQL.Company](sqlContext, ReadConfig(Map("collection" -> "startUps"), Some(ReadConfig(sqlContext)))).show()

    // Drop database if you want
    //MongoConnector(sc).withDatabaseDo(ReadConfig(sc), db => db.drop())

    //changing sparkContext
    val newUri = "mongodb://localhost/jyothi.test" //updating to test collection
    val newSparkContext = new SparkContext(sc.getConf
      .set("spark.ui.port", "23456") //custom port
      .setAppName("SparkMongoScalaPlayTest") //new name
      .set("spark.app.id", "SparkMongoScalaPlayTest") //new Id
      .set("spark.mongodb.input.uri", newUri)
      .set("spark.mongodb.output.uri", newUri)
    )

    // raw data
    val docs = """
                 |{"name": "Bilbo Baggins", "age": 50}
                 |{"name": "Gandalf", "age": 1000}
                 |{"name": "Thorin", "age": 195}
                 |{"name": "Balin", "age": 178}
                 |{"name": "Kíli", "age": 77}
                 |{"name": "Dwalin", "age": 169}
                 |{"name": "Óin", "age": 167}
                 |{"name": "Glóin", "age": 158}
                 |{"name": "Fíli", "age": 82}
                 |{"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
    //custom write config
    val writeConfig = WriteConfig(Map("collection" -> "test", "writeConcern.w" -> "majority"), Some(WriteConfig(newSparkContext)))
    MongoSpark.save(newSparkContext.parallelize(docs.map(Document.parse)), writeConfig)

    //new sqlContext from new SparkContext
    val newSqlContext = SQLContext.getOrCreate(newSparkContext)

    // Explicitly declaring a schema
    MongoSpark.load[SparkSQL.Character](newSqlContext).printSchema()

    // Spark SQL
    val characters = MongoSpark.load[SparkSQL.Character](newSqlContext)
    characters.registerTempTable("characters")

    // Using the SQL helpers and StructFields helpers
    val objectId = "123400000000000000000000"
    val newDocs = Seq(new Document("_id", new ObjectId(objectId)).append("a", 1), new Document("_id", new ObjectId()).append("a", 2))
    MongoSpark.save(newSparkContext.parallelize(newDocs))

    // Set the schema using the ObjectId StructFields helper
    import org.apache.spark.sql.types.DataTypes
    import com.mongodb.spark.sql.helpers.StructFields

    val schema = DataTypes.createStructType(Array(
      StructFields.objectId("_id", nullable = false),
      DataTypes.createStructField("a", DataTypes.IntegerType, false))
    )

    // Create a dataframe with the helper functions registered
    val dataFrameWithHelpers = MongoSpark.read(sqlContext).schema(schema).option("registerSQLHelperFunctions", "true").load()

    // Query using the ObjectId string
    dataFrameWithHelpers.filter(s"_id = ObjectId('$objectId')").show()

  }

  case class Character(name: String, age: Int)

  case class Company(name: String, founded_year: Int, number_of_employees: Int) //TODO: define Company case class as model for company document

}