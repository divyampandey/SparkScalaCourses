package com.thelearningjournals.spark
import org.apache.spark._
import org.apache.log4j._

import scala.math._


object mymintemperature {
    def myparser(line:String): (String, String,Float) ={
        val myline = line.split(",")
        val stationid = myline(0)
        val entrytype = myline(2)
        val temp = myline(3).toFloat * 0.1f
        return(stationid,entrytype,temp)
    }
    def main(args: Array[String]): Unit ={
        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = new SparkContext("local[*]","mymintemperature")
        val onerdd = sc.textFile("data/1800.csv")
        val twordd = onerdd.map(myparser)
        val filterrdd = twordd.filter(x=>x._2=="TMIN")
        val filtermaxrdd = twordd.filter(x=>x._2=="TMAX")
        val fourrdd = filtermaxrdd.map(x=>(x._1,x._3))
        val fiverdd = fourrdd.reduceByKey((x,y)=>max(x,y))
        val resultmax=fiverdd.collect()
        val threerdd = filterrdd.map(x=>(x._1,x._3))
        val finalrdd = threerdd.reduceByKey((x,y)=>min(x,y))
        val resultmin = finalrdd.collect()
        for(x<-resultmax.sorted){
            val station = x._1
            val temp = x._2
            val formatted = f"$station has maximum temperature of $temp%.02f"
            println(formatted)
        }
      for(x<-resultmin.sorted){
        val station = x._1
        val temp = x._2
        val formatted = f"$station has minimum temperature of $temp%.02f"
        println(formatted)
      }

    }
}
