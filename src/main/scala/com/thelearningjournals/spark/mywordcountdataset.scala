package com.thelearningjournals.spark
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object mywordcountdataset {
      Logger.getLogger("org").setLevel(Level.ERROR)
      case class Book(value:String)

      def main(args: Array[String]): Unit ={
          val spark = SparkSession.builder().appName("mywordcount").master("local[*]").getOrCreate()
          import spark.implicits._
          val mydataframe = spark.read.text("data/book.txt").as[Book]
          val mydataset = mydataframe.select(explode(split($"value","\\W+")).alias("word")).groupBy("word").count()
        val mydataset1 = mydataframe.select(explode(split($"value","\\W+")).alias("word")).groupBy(lower($"word")).count().sort("count")
        mydataset1.show(mydataset1.count.toInt)
        spark.close()
      }
}
