package com.thelearningjournals.spark
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._


object myavgfriendsbyagedataset {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]): Unit ={
      val spark = SparkSession.builder().appName("myspark").master("local[*]").getOrCreate()
     val mydataset = spark.read.option("inferSchema","true").option("header","true").csv("data/fakefriends.csv")
     mydataset.groupBy("age").avg("friends").show()
     mydataset.groupBy("age").agg(round(avg("friends"),2)).show()
     mydataset.groupBy("age").agg(round(avg("friends"),2)).alias("friend_by_avg").sort("age").show()
     spark.close()
  }


}
