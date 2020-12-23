package com.thelearningjournals.spark
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j._


object myspentbycustomerdataset {
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class Customer_order(customer_id:Int,order_id:Int,transaction_amt:Float)
  def main(args:Array[String]): Unit ={
      val spark = SparkSession.builder().appName("mysparkcustomer").master("local[*]")
        .getOrCreate()
    val myschema = new StructType()
      .add("customer_id",IntegerType,true)
      .add("order_id",IntegerType,true)
      .add("transaction_amt",FloatType,false)
    import spark.implicits._
    val mydataset = spark.read.format("csv").schema(myschema).load("data/customer-orders.csv").as[Customer_order]
    mydataset.show()
    val result = mydataset.groupBy("customer_id").sum("transaction_amt").sort("sum(transaction_amt)")
    result.select(col("customer_id"),round(col("sum(transaction_amt)").alias("sum_amt"),2)).show(result.count.toInt)
    spark.close()
  }

}
