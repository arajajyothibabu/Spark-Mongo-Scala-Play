package controllers

import play.api._
import play.api.mvc._
import utils.{MongoUtility, SparkSQL, SparkStreams}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class Application extends Controller {

  def index = Action {
    //Uncomment only one at a time for better understanding
    //println("**********************MongoUtility*********************")
    //Future{MongoUtility.main(new Array[String](0))}

    println("*******************SparkSQL*********************")
    Future{SparkSQL.main(new Array[String](0))}

    //println("*******************SparkStreams*********************")
    //Future{SparkStreams.main(new Array[String](0))}

    Ok(views.html.index("Your new application is ready."))

  }

}