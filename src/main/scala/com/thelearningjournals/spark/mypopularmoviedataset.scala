package com.thelearningjournals.spark
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object mypopularmoviedataset {
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class movie(movie_id:Int)
  case class movie_info(movie_id:Int,movie_name:String)
  def main(args:Array[String]): Unit ={
      val spark = SparkSession
        .builder()
        .appName("sparkmovie")
        .master("local[*]")
        .getOrCreate()

      val movie_schema = new StructType()
        .add("user_id",IntegerType,true)
        .add("movie_id",IntegerType,true)
        .add("ratings",IntegerType,true)
        .add("timestamp",IntegerType,true)

      val movie_name_schema = new StructType()
        .add("movie_id",IntegerType,true)
        .add("movie_name",StringType,true)
        .add("genre",StringType,true)






     import spark.implicits._
     val movieDataSet = spark.read.option("sep","::").schema(movie_schema).csv("data/ml-10M/ratings.dat").as[movie]
      //most popular movie i.e the movie which has most no. of distinct ratings
     val movieInfoData = spark.read.option("sep","::").schema(movie_name_schema).csv("data/ml-10M/movies.dat").as[movie_info]
    val results = movieDataSet.groupBy("movie_id").count().orderBy(desc("count"))
    val joinedDataSet = results.as("r")
      .join(movieInfoData.as("m"),$"r.movie_id"===$"m.movie_id","left")
      .select($"r.movie_id",$"r.count",$"m.movie_name",$"m.genre")
    val answer = joinedDataSet.select("movie_id","count","movie_name","genre").orderBy(desc("count"))
    answer.show(truncate=false)


    spark.close()
  }
}
