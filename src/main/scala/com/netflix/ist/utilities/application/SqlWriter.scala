package com.netflix.ist.utilities.application

import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql._
import oracle.jdbc.pool.OracleDataSource

object SqlWriter extends App{

    try {
      val ods = new OracleDataSource()
      ods.setUser("username")
      ods.setPassword("giveyourpassword")
      ods.setURL("jdbc:oracle:thin:@//this.is.dummy.url:1981/sid")

      val con = ods.getConnection()
      println("Connected")

      val s1 = con.createStatement()

      val out = s1.executeQuery("SELECT * FROM SNoah")

      while (out.next()){
        print(out.getString(1))
        print("\n")
      }

      print(out)
    }catch {
      case e: SQLException => {
        println("Connection Failed")
        println(e.getStackTrace)
      }
      case e : Exception => print("error has occured")
    }
}


