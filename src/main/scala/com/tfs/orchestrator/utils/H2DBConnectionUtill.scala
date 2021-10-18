package com.tfs.orchestrator.utils

import java.sql._

import org.apache.logging.log4j.scala.Logging

object H2DBConnectionUtil extends Logging{

  private var connection : Connection = _
  private var statement :Statement = _

  def evaluateSqlExpression(sql:String): Boolean ={
    try {
      Class.forName(Constants.JDBC_DRIVER)
      connection = DriverManager.getConnection(Constants.DB_URL, Constants.USER, Constants.PASS)
      statement = connection.createStatement
      val rs = statement.executeQuery(sql)
      var result = false
      while (rs.next) {
        result = rs.getBoolean(1)
      }
      statement.close()
      connection.close()
      result
    } catch {
      case e:Exception => logger.error("error while Evaluating SQL Expression", e)
        false
    }
    finally {
      //finally block used to close resources
      try{
        if(statement !=null) statement.close()
      } catch {
        case e:SQLException => logger.error("Error while closing Statement ", e)
      }
      try {
        if(connection != null) connection.close()
      } catch {
        case e:SQLException => logger.error("Error while closing connection ", e)
      }
    }
    }
}
