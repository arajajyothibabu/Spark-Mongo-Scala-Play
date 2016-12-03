name := "SparkMongoScalaPlay"

version := "1.0"

lazy val `sparkmongoscalaplay` = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  jdbc ,
  cache ,
  ws   ,
  "org.apache.spark" % "spark-core_2.11" % "1.6.2",
  "org.apache.spark" % "spark-sql_2.11" % "1.6.2",
  "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.0.0",
  specs2 % Test
)

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )  

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"  