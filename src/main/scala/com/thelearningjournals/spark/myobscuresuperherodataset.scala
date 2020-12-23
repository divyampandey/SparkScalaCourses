package com.thelearningjournals.spark
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object myobscuresuperherodataset {
    Logger.getLogger("org").setLevel(Level.ERROR)
    case class SuperHero(heros:String)
    case class SuperHeroInfo(id:Int,name:String)

    def main(args:Array[String]): Unit ={
        val spark = SparkSession.builder()
          .appName("obscurehero")
          .master("local[*]")
          .getOrCreate()

        val heroSchema = new StructType()
          .add("heros",StringType,true)

        val heroinfoSchema = new StructType()
          .add("id",IntegerType,true)
          .add("name",StringType,true)

        import spark.implicits._
        val heroDS = spark.read.schema(heroSchema).text("data/Marvel-graph.txt").as[SuperHero]
        val heroInfo = spark.read.option("sep"," ").schema(heroinfoSchema).csv("data/Marvel-names.txt").as[SuperHeroInfo]


        val newDS = heroDS.withColumn("heroid",split(col("heros")," ")(0))
          .withColumn("count",size(split(col("heros")," "))-1).groupBy("heroid").sum("count")


        val minimum = newDS.agg(min("sum(count)")).first()(0)

        val newDS2 = newDS.select("heroid").where($"sum(count)"===minimum)

        val result = heroInfo.join(newDS2,heroInfo("id")===newDS("heroid"),"inner").select("id","name")

        result.show(false)


    }

}
