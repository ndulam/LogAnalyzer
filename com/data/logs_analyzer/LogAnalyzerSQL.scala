package com.data.logs_analyzer


import java.io.{File, FileReader, BufferedReader}
import java.text.SimpleDateFormat
import java.util.Date

import com.datamantra.utils.{AccessLogUtils, ApacheAccessLog}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
 * LogAnalyzerSQL shows how to use SQL syntax with Spark.
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.databricks.apps.logs.chapter1.LogAnalyzerSQL"
 *   --master local[4]
 *   target/scala-2.10/spark-logs-analyzer_2.10-1.0.jar
 *   ../../data/apache.access.log
 */


object LogAnalyzerSQL {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster(args(0))setAppName("Log Analyzer SQL in Scala")
    val sc = new SparkContext(sparkConf)

    val logFile = args(1)
    val countryCodBrodcast = sc.broadcast(AccessLogUtils.createCountryCodeMap(args(2)))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).toDF()
    val allaccessLogs = sc.textFile(logFile).map(record => {
      val parseResult = ApacheAccessLog.parseLogLine(record)
      if (parseResult.isRight)
        (true, parseResult.right.get)
      else
        (false, record)
    })

    val normalRecords = allaccessLogs.filter(_._1 == true).map(_._2)
    val errorRecords = allaccessLogs.filter(_._1 == false).map(_._2)

    /*val accessLogsDF  = normalRecords.map(record => record match {
      case x: ApacheAccessLog => x
    }).toDF() */

    val accessLogs  = normalRecords.map(record => {record match {
      case x:ApacheAccessLog => x}
    })

    val refinedLogsDF = accessLogs.map(log => AccessLogUtils.getViewedProducts(log, countryCodBrodcast)).toDF()

    //accessLogsDF.registerTempTable("accessLog")
    refinedLogsDF.registerTempTable("viewed_products")
    sqlContext.cacheTable("viewed_products")

    //PageViews per user
    val pageViewsPerUser = sqlContext
      .sql("SELECT user, COUNT(*) FROM viewed_products GROUP BY user")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    println(s""" Page Views Per User: ${pageViewsPerUser.mkString("[", ",", "]")}""")


    val pageViewsPerCountry = sqlContext
      .sql("SELECT cntryCode, COUNT(*) FROM viewed_products GROUP BY cntryCode")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    println(s""" Page Views Per Country: ${pageViewsPerCountry.mkString("[", ",", "]")}""")


    // Page views per hour
    sqlContext.udf.register("getHour", AccessLogUtils.getHour _)
    val pageViewsPerHour = sqlContext
      .sql("SELECT hr, count(products) FROM (SELECT getHour(dateTime) AS hr, products FROM viewed_products) t GROUP BY hr")
      .map(row => (row.getInt(0), row.getLong(1)))
      .collect()
    println(s""" Page Views Per Hour: ${pageViewsPerHour.mkString("[", ",", "]")}""")


    //Top Product Viewed

    val topProducts = sqlContext
      .sql("SELECT products, COUNT(*) AS total FROM viewed_products GROUP BY products ORDER BY total DESC LIMIT 10")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    println(s""" Top products: ${topProducts.mkString("[", ",", "]")}""")

    //Top products viewed per user;

    val topProductsPerUser = sqlContext
      .sql("SELECT user, products, COUNT(*) as total FROM viewed_products GROUP BY user, products ORDER BY total DESC")
      .map(row => (row.getString(0), row.getString(1), row.getLong(2)))
      .collect()
    println(s""" Top products viewed per user: ${topProductsPerUser.mkString("[", ",", "]")}""")


    // Calculate statistics based on the content size.
    /*
    val contentSizeStats = sqlContext
      .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM accessLog")
      .first()
    println("Content Size Avg: %s, Min: %s, Max: %s".format(
      contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
      contentSizeStats(2),
      contentSizeStats(3)))

    // Compute Response Code to Count.
    val responseCodeToCount = sqlContext
      .sql("SELECT responseCode, COUNT(*) FROM accessLog GROUP BY responseCode LIMIT 1000")
      .map(row => (row.getInt(0), row.getLong(1)))
      .collect()
    println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

    // Any IPAddress that has accessed the server more than 10 times.
    val ipAddresses =sqlContext
      .sql("SELECT ipAddress, COUNT(*) AS total FROM accessLog GROUP BY ipAddress HAVING total > 10 LIMIT 1000")
      .map(row => row.getString(0))
      .collect()
    println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

    val topEndpoints = sqlContext
      .sql("SELECT endpoint, COUNT(*) AS total FROM accessLog GROUP BY endpoint ORDER BY total DESC LIMIT 10")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    println(s"""Trending Products: ${topEndpoints.mkString("[", ",", "]")}""")
*/
    sc.stop()
  }
}
