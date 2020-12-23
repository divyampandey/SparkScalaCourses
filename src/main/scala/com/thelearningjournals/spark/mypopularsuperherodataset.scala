package com.thelearningjournals.spark
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object mypopularsuperherodataset {
    //Restrict unnecessary INFO Show only Error
    Logger.getLogger("org").setLevel(Level.ERROR)

    case class SuperHero(superherostring:String)
    case class SuperHeroInfo(heroId:Int, heroName:String)
    //scala function to extract hero_id and co-heroes

    //main function
    def main(args:Array[String]): Unit ={
      // create spark session
      val spark = SparkSession.builder()
        .appName("popularSuperHero")
        .master("local[*]")
        .getOrCreate()

      val superheroSchema = new StructType()
        .add("superherostring",StringType,true)

      val superheroInfoSchema = new StructType()
        .add("heroId",IntegerType,true)
        .add("heroName",StringType,true)


      import spark.implicits._
      //Load the data into spark dataset.
      val lines = spark.read.schema(superheroSchema).text("data/Marvel-graph.txt").as[SuperHero]
      val superherods = lines.withColumn("id",split(col("superherostring")," ")(0))
        .withColumn("connection_count",size(split(col("superherostring")," "))-1)
      val finedataset = superherods.select("id","connection_count").groupBy("id").sum("connection_count").orderBy(desc("sum(connection_count)")).first()
      val pheroid = finedataset(0)
      val maxoccur = finedataset(1)

      val heroinfo = spark.read.option("sep"," ").schema(superheroInfoSchema).csv("data/Marvel-names.txt").as[SuperHeroInfo]
      val result = heroinfo.select("heroName").where($"heroId"===pheroid).first().getString(0)
      println(result.getClass)
      println(s"Most popular superhero is $result with maximum co-occurences of $maxoccur")



    }
}
