package com.phylosoft.spark.learning.sql.streaming.sink.foreach

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * Created by Andrew on 5/20/2018.
  */
class ForeachJDBCWriter(url: String, user: String, pwd: String, table: String) extends ForeachWriter[Row] {

  val driver = "org.postgresql.Driver"

  var connection: Connection = _
  var statement: PreparedStatement = _
  private val sql = "INSERT INTO %s (a, b, c, d, e, f, h ".format(table) +
    "VALUES (?, ?, ?, ?, ?, ?, ?)"

  val calendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

  override def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.prepareStatement(sql)
    true
  }

  override def process(row: Row): Unit = {
    if (row.getBoolean(row.fieldIndex("expired"))) {
      statement.setString(1, row.getAs[String]("a"))
      statement.setString(2, row.getAs[String]("b"))
      statement.setString(3, row.getAs[String]("c"))
      statement.setString(4, row.getAs[String]("d"))
      statement.setInt(5, row.getAs[Int]("e"))
      statement.setTimestamp(6, new Timestamp(row.getAs[Long]("f")), calendar)
      statement.setTimestamp(7, new Timestamp(row.getAs[Long]("h")), calendar)
      statement.addBatch()
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    // close the connection
    statement.executeBatch
    statement.close
    connection.close
  }

}
