package main

import org.apache.spark.sql.SparkSession


object Detector {
  def main(args: Array[String]) {
  val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()
                                  

        val df= sparkSession.read.option("header","true").csv("Data/periodictable.csv")
         df.show()
    
    }
}