package com.thelearningjournals.spark
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._

object mydatasetdataframe1 {
    Logger.getLogger("org").setLevel(Level.ERROR)
    case class Person(id:Int,name:String,age:Int,freinds:Int)
    def main(args : Array[String]): Unit ={
        val spark = SparkSession.builder().appName("myspark").master("local[*]").getOrCreate()
        val schema = new StructType().add("id",IntegerType,true)
          .add("name",StringType,true).add("age",IntegerType,true).add("friends",IntegerType,true)
        import spark.implicits._
        val mydataset = spark.read.format("csv").option("header","true").schema(schema).load("data/fakefriends.csv")
        val mydataframe = spark.read.option("header","true").csv("data/fakefriends.csv")
        mydataset.show()
        mydataset.select("name").show()
        mydataset.filter(mydataset("age")<21).show()
        mydataset.select(mydataset("name"),mydataset("age")+10).show()
        mydataset.groupBy("age").count().show()

        println(mydataframe.getClass)
        mydataframe.show()
        spark.close()
    }
}
