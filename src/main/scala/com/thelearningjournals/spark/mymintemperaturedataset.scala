package com.thelearningjournals.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{types, _}
import org.apache.log4j._
import org.apache.spark.sql.types._



object mymintemperaturedataset {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit ={
      val spark = SparkSession.builder().appName("mysparkmintemp").master("local[*]").getOrCreate()
      val myschema = new StructType()
        .add("station_id",StringType,true)
        .add("time_in_int",IntegerType,true)
        .add("Temp_type",StringType,true)
        .add("temperature",IntegerType,true)

      val mydataframe = spark.read.format("csv").schema(myschema).load("data/1800.csv")
      mydataframe.show()
      import spark.implicits._
      val mydataframe1 = mydataframe.filter($"Temp_type"==="TMIN").groupBy("station_id").min("temperature").select(col("station_id"),((col("min(temperature)")*0.1f *(9.0f/5.0f))+32.0f).alias("min_temp "))

      mydataframe1.show()
      spark.close()
  }

}
