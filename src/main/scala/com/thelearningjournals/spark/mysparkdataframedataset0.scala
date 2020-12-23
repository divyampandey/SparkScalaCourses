package com.thelearningjournals.spark

import org.apache.spark.sql._
import org.apache.log4j._

object mysparkdataframedataset0 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person(id:Int,name:String,age:Int,friends:Int)

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder().appName("firstsparksession").master("local[*]").getOrCreate()
    import spark.implicits._
    val mydataset = spark.read.option("inferSchema","true").option("header","true").csv("data/fakefriends.csv").as[Person]

    println(mydataset.printSchema())

    mydataset.createOrReplaceTempView("People")

    val eighteen = spark.sql("Select age,count(*) as cnt from People group by 1 order by cnt desc")

    eighteen.show()
    spark.close()

  }
}
