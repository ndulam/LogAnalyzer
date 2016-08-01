package com.data.logs_analyzer

import com.datamantra.logs_analyzer.cassandra.dao.LogEventDAO
import com.datamantra.utils.{AccessLogUtils, ApacheAccessLog}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * The LogAnalyzerStreaming illustrates how to use logs with Spark Streaming to
 *   compute statistics every slide_interval for the last window length of time.
 *
 * To feed the new lines of some logfile into a socket, run this command:
 *   % tail -f [YOUR_LOG_FILE] | nc -lk 9999
 *
 * If you don't have a live log file that is being written to, you can add test lines using this command:
 *   % cat ../../data/apache.access.log >> [YOUR_LOG_FILE]
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreaming"
 *   --master local[4]
 *   target/scala-2.10/spark-logs-analyzer_2.10-1.0.jar
 */
object LogAnalyzerSocketStreaming {
  //val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)

  def main(args: Array[String]) {

    val master = args(0)
    val lookupFile = args(1)
    val port = args(2).toInt
    val cassandraNodes = args(3)
    val cassandraKeySpace = args(4)
    val cassandraRepFactor = args(5).toInt

    val logEventDAO = new LogEventDAO()
    logEventDAO.init(cassandraNodes.split(",").toSeq, cassandraKeySpace)
    logEventDAO.createSchema(cassandraRepFactor.toInt)


    val sparkConf = new SparkConf().setMaster(master)setAppName("Log Analyzer Streaming in Scala")
    val sc = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL)

    val countryCodBrodcast = sc.broadcast(AccessLogUtils.createCountryCodeMap(lookupFile))
    val logLinesDStream = streamingContext.socketTextStream("localhost", port)


    //val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()
    val accessLogsDStream = logLinesDStream.map(record => {
      val parseResult = ApacheAccessLog.parseLogLine(record)
      if (parseResult.isRight)
        (true, parseResult.right.get)
      else
        (false, record)
    })

    val normalRecords = accessLogsDStream.filter(_._1 == true).map(_._2)
    val errorRecords = accessLogsDStream.filter(_._1 == false).map(_._2)

    val accessLogs  = normalRecords.map(record => record match {
      case x: ApacheAccessLog => x
    })

    val refinedLogs = accessLogs.map(log => AccessLogUtils.getViewedProducts(log, countryCodBrodcast))

    //val windowDStream = refinedLogs.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    refinedLogs.foreachRDD(accessLogs => {
      val productViewCount = accessLogs
        .map(accesslog => accesslog.products)
        .countByValue()
      productViewCount.foreach(result => logEventDAO.updatePageViews(result._1, result._2.toInt))

      val responseCodeCounts = accessLogs
        .map(accesslog => accesslog.responseCode)
        .countByValue()
      responseCodeCounts.foreach(result => logEventDAO.updateStatusCounter(result._1, result._2))

      val productViewsByCntry = accessLogs
        .map(accessLogs => accessLogs.cntryCode)
        .countByValue()
      productViewsByCntry.foreach(result => logEventDAO.updateVisitsByCountry(result._1, result._2.toInt))

      val logVolumePerMinute = accessLogs
        .map(accessLog => AccessLogUtils.getMinPattern(accessLog.dateTime))
        .countByValue()
      logVolumePerMinute.foreach(result => logEventDAO.updateLogVolumeByMinute(result._1, result._2.toInt))

    })


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
