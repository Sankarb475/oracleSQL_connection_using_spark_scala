package com.netflix.ist.utilities.application


import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.SparkContext


object SqlWriterSpark extends App{

  try {
    val statusDF = Seq(120)

    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val sc: SparkContext = spark.sparkContext

    val outDF = sc.parallelize(statusDF).toDF("ID")

    val props = new java.util.Properties
    props.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    props.setProperty("user", "username")
    props.setProperty("password", "giveyourpassword")

    val url: String = "jdbc:oracle:thin:@//this.is.dummy.url:1981/sid"

    //destination database table
    val table = "SNoah"

    //write data from spark dataframe to database
    outDF.write.mode("append").jdbc(url, table, props)
  } catch {
    case e : Exception => {
      print("connection error")
    }
  }

}
