package com.thelearningjournals.spark

import org.apache.spark._
import org.apache.log4j._




object mybetterwordcount {
      def main(args : Array[String]): Unit ={

        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = new SparkContext("local[*]","mybetterwordcount")
        val myrdd = sc.textFile("data/book.txt")
        val words = myrdd.flatMap(x=>x.split("\\W+"))


        val finalword = words.map(x=>x.toLowerCase())
        val ans1 = finalword.map(x=>(x,1)).reduceByKey((x,y)=>x+y)
        val ans2 = ans1.reduceByKey((x,y)=>x+y)

          ans2.foreach(println)
        println("-----------------------------------------------")





      }
}
