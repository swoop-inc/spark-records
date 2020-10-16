package com.swoop.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local[2]").appName("spark-records").config("spark.sql.shuffle.partitions", "4").getOrCreate()
  }

}
