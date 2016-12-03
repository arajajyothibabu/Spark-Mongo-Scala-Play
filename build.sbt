name := "SparkMongoScalaPlay"

version := "1.0"

lazy val `sparkmongoscalaplay` = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

val sparkVersion = "1.6.2"

libraryDependencies ++= Seq(
  jdbc ,
  cache ,
  ws   ,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.0.0",
  specs2 % Test
)

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )  

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"  