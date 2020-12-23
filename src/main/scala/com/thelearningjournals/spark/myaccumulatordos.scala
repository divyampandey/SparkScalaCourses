package com.thelearningjournals.spark
import org.apache.spark._
import org.apache.log4j._

import scala.collection.mutable.ArrayBuffer

object myaccumulatordos {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val characterid = 859
  val finalid = 5310
  def convert_to_node(line:String):(Int,(ArrayBuffer[Int],Int,String)) ={
      val fields = line.split(" ")
      val heroid = fields(0).toInt

      var myarray = new ArrayBuffer[Int]()
      for(x<-1 until fields.length -1 ){
        myarray += fields(x).toInt
      }
    var distance = -1
    var color = "WHITE"
    if(heroid == characterid){
      distance = 0
      color = "GRAY"

    }
   return (heroid,(myarray,distance,color))

  }

  def bfsreduce(line:(ArrayBuffer[Int],Int,String),line1:(ArrayBuffer[Int],Int,String)): (ArrayBuffer[Int],Int,String) ={
      val buffer1 = line._1
      val buffer2 = line1._1
      val finalbuffer = buffer1 ++ buffer2
      val distance = -1
    val color = "WHITE"
    return(finalbuffer,distance,color)

  }

  //load the graph-data
  def main(args:Array[String]): Unit ={
    val sc = new SparkContext("local[*]","dos")
    val superhero = sc.textFile("data/Marvel-graph.txt")
    val mygraph = superhero.map(convert_to_node)
    val myreducegraph = mygraph.reduceByKey(bfsreduce)
     myreducegraph.foreach(println)



  }

}
