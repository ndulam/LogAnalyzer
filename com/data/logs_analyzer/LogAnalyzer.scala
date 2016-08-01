package com.data.logs_analyzer


import java.io.{File, BufferedReader, FileReader}
import java.text.SimpleDateFormat

import com.data.utils.{AccessLogUtils, OrderingUtils, ApacheAccessLog}
import org.apache.spark.{ SparkContext, SparkConf}

import scala.collection.mutable



/**
 * The LogAnalyzer takes in an apache access log file and
 * computes some statistics on them.
 *
 */
object LogAnalyzer {

  def toDate(sourceDate: String, srcPattern: String, destPattern: String): String = {
    val srcdf = new SimpleDateFormat(srcPattern)
    val destdf = new SimpleDateFormat(destPattern)
    val d = srcdf.parse(sourceDate)
    val result = destdf.format(d)
    result
  }

  def geProducts(requestURI: String): String = {
    val parts = requestURI.split("/")
    parts(3)
  }

  def getCountryCodeMap(filename: String): mutable.HashMap[String, String] = {

    val countryCodeMap = mutable.HashMap[String, String]()
    val bufferedReader = new BufferedReader(new FileReader(new File(filename)))
    var line: String = null
    while ({
      line = bufferedReader.readLine
      line
    } != null) {

      val pair = line.split(" ")
      countryCodeMap.put(pair(0), pair(1))
    }
    bufferedReader.close()
    countryCodeMap
  }

  def getCountryCode(ipaddress: String): String = {
    val octets = ipaddress.split("\\.")
    val classABIP = octets(0) + "." + octets(1)

    classABIP
  }

  val printfn = (key: String, value: Iterable[String]) => {

    print("Key: " + key + "Value: " + value.toList)

  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster(args(0))setAppName "Log Analyzer in Scala"
    val sc = new SparkContext(sparkConf)

    val logFile = args(1)
    val countryCodBrodcast = sc.broadcast(getCountryCodeMap(args(2)))


    /*
    val allaccessLogs = sc.textFile(logFile).map(record => {
      val parseResult = ApacheAccessLog.parseLogLine(record)
      if (parseResult.isRight)
        (true, parseResult.right.get)
      else
        (false, record)
    })


    val normalRecords = allaccessLogs.filter(_._1 == true).map(_._2)
    val errorRecords = allaccessLogs.filter(_._1 == false).map(_._2)


    val accessLogs  = normalRecords.map(record => record match {
      case x: ApacheAccessLog => x
    })
*/

    val allaccessLogs = sc.textFile(logFile, 10).map(record => ApacheAccessLog.parseLogLine(record))
    //val normalLogs = allaccessLogs.filter(_.isRight).map(_.right.get)

    //val allaccessLogs = sc.textFile(logFile).map(log => ApacheAccessLog.parseLogLine(log))
    //allaccessLogs.mapPartitions()
    val accessLogs = allaccessLogs.collect({
      case t if t.isRight => t.right.get
    }).cache()

    val errorLogs = allaccessLogs.collect({
      case t if t.isLeft => t.left.get
    })

    val refinedLogs = accessLogs.map(log => AccessLogUtils.getViewedProducts(log, countryCodBrodcast)).cache()

    // Do some computing here


    //PageViews per user
    val pageViewsPerUser = refinedLogs
      .map(log => (log.user, 1))
      .reduceByKey(_ + _)
      .take(100)
    println(s"""PageViews per user: ${pageViewsPerUser.mkString("[", ",", "]")}""")

    //Page views per country
    val pageViewsPerCntry = refinedLogs
      .map(log => (log.cntryCode, 1))
      .reduceByKey(_ + _)
      .take(100)
    println(s"""PageViews per country: ${pageViewsPerCntry.mkString("[", ",", "]")}""")

    // Page views per hour
    val pageViewsPerHour = refinedLogs
      .map(log => (AccessLogUtils.getHour(log.dateTime), 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .take(100)
    println(s"""PageViews per hour: ${pageViewsPerHour.mkString("[", ",", "]")}""")

    //Top Products Viewed
    val topProductsViewed = refinedLogs
      .map(log => (log.products, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(100)
    println(s"""Top Products Viewed: ${topProductsViewed.mkString("[", ",", "]")}""")

    //Top products viewed per user;
    val topProductsViewedPerUser = refinedLogs
      .map(log => ((log.user, log.products), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(100)
    println(s"""Top Products Viewed Per User: ${topProductsViewedPerUser.mkString("[", ",", "]")}""")

    /*

    // Calculate statistics based on the content size.
    val contentSizes = accessLogs.map(log => log.contentSize)
    println("Content Size Avg: %s, Min: %s, Max: %s".format(
      contentSizes.reduce(_ + _) / contentSizes.count,
      contentSizes.min,
      contentSizes.max))

    // Compute Response Code to Count.
    val responseCodeToCount = accessLogs
      .map(log => (log.responseCode, 1))
      .reduceByKey(_ + _)
      .take(100)
    println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

    // Any IPAddress that has accessed the server more than 10 times.
    val ipAddresses = accessLogs
      .map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 10)
      .map(_._1)
      .take(100)
     println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

    // Top Endpoints.
    val topEndpoints = accessLogs
      .map(log => (log.requestURI, 1))
      .reduceByKey(_ + _)
      .top(10)(OrderingUtils.SecondValueOrdering)
    println(s"""Trending Products: ${topEndpoints.mkString("[", ",", "]")}""")
*/
    println(errorLogs.collect()toList)
    sc.stop()
  }
}
