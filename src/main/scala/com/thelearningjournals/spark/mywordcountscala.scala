package com.thelearningjournals.spark
import org.apache.spark._
import org.apache.log4j._



object mywordcountscala {


      def myparser(word:String):String={
          val newword = word.replaceAll("[\'0-9:,.?!;\"-]+","")
          return newword

      }
      def main(args: Array[String]): Unit ={
        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = new SparkContext("local[*]","mywordcount")
        val lines = sc.textFile("data/book.txt")
        val words  = lines.flatMap(x=>x.split(' '))
        val word = words.map(myparser)
        val finalwords = word.map(x=>(x,1))
        val results = finalwords.reduceByKey((x,y)=>(x+y))
        results.foreach(println)



      }
}
