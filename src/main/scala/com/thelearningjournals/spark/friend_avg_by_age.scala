package com.thelearningjournals.spark
import org.apache.spark._
import org.apache.log4j._


object friend_avg_by_age {
    def myparser(line:String): (Int, Int) ={
        val rdd =  line.toString().split(",")
        val age = rdd(2).toInt
        val no_of_frnd = rdd(3).toInt
        return(age,no_of_frnd)

    }
    def main(args: Array[String]): Unit ={
        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = new SparkContext("local[*]","myfriendage")
        val rdd = sc.textFile("data/fakefriends-noheader.csv")
        val results = rdd.map(myparser)
        val avgage = results.mapValues(x=>(x,1))
        val res = avgage.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
        val fres = res.map(x=>(x._1,x._2._1/x._2._2))
        fres.foreach(println)

    }
}
