package com.thelearningjournals.spark

import org.apache.spark._
import org.apache.log4j._

object rddfirstexample {
    def main(args: Array[String]): Unit ={
        Logger.getLogger("org").setLevel(Level.OFF)
        val sc = new SparkContext(master="local[*]","ratingcount")
        val lines = sc.textFile("data/ml-100k/u.data")
        val ratings = lines.map(x=> x.toString().split('\t')(2))
        val reduceit = ratings.countByValue()
        reduceit.foreach(println)
        val sorted = reduceit.toSeq.sortBy(_._1)
        println("---------------------------SORTED----------------------------------")
        for(rating<-sorted){println(rating)}



    }
}
