package com.thelearningjournals.spark
import org.apache.spark._
import org.apache.log4j._



object myspentbycustomer {

  def myownparser(line:String):(String,Float)={
      val fline = line.split(",")
      val customer_id = fline(0)
      val item_id = fline(1)
      val price = fline(2).toFloat
    (customer_id,price)
  }


  def main(args: Array[String]): Unit ={
      Logger.getLogger("org").setLevel(Level.ERROR)
      val sc = new SparkContext("local[*]","myspentbycustomer")
      val customerRdd = sc.textFile("data/customer-orders.csv")
      val customerPriceRdd = customerRdd.map(myownparser)
      val resultRdd = customerPriceRdd.reduceByKey((x,y)=>(x+y)).map(x=>(x._2,x._1)).sortByKey(false)
      val resultRdd1 = resultRdd.take(20)
    for(x<-resultRdd1){

          val customer = x._2
          val price = x._1
          println(f"customer_nbr: $customer bought items total of $price%.2f"+"$")

      }


  }

}
