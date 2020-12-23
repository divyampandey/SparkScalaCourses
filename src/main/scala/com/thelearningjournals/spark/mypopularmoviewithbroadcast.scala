package com.thelearningjournals.spark
import java.nio.file.Files.lines

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, desc, udf}
import org.apache.log4j._
import org.apache.spark.sql.types._

import scala.io.{Codec, Source}

object mypopularmoviewithbroadcast {

  Logger.getLogger("org").setLevel(Level.ERROR)
  case class movie(movie_id:Int)

  def load_movie_info(path:String): scala.collection.mutable.Map[Int,String] ={
     implicit val codec = Codec("ISO-8859-1")
      val dictionary = collection.mutable.Map[Int,String]()
      for(line <- Source.fromFile(path).getLines){
          val parts = line.split('|')
            val movie_id = parts(0).toInt
            val movie_name = parts(1).toString
            dictionary +=(movie_id->movie_name)
      }
      return (dictionary)

  }
  def main(args:Array[String]): Unit ={

    val maindict = load_movie_info("data/ml-100k/u.item")
    val spark = SparkSession.builder().appName("mysparkbroadcast").master("local[*]").getOrCreate()
    val bdict = spark.sparkContext.broadcast(maindict)
    val movieschema = new StructType()
        .add("user_id",IntegerType,true)
        .add("movie_id",IntegerType,true)
        .add("ratings",IntegerType,true)
        .add("timestamp",IntegerType,true)
    import spark.implicits._
    val dataset = spark.read.option("sep","\t").schema(movieschema).csv("data/ml-100k/u.data").as[movie]
    val results = dataset.select("movie_id").groupBy("movie_id").count()
    val lookupname : Int => String = (movieid:Int)=>{
      bdict.value(movieid)
    }

    val lookupfunc =udf(lookupname)
    val answer = results.withColumn("movie_name",lookupfunc(col("movie_id")))
    answer.select(col("movie_id"),col("movie_name"),col("count")).orderBy(desc("count")).show(truncate = false)
    spark.close()

  }

}
