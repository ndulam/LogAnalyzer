package com.data.loggenerator

import java.io.{DataOutputStream, FileOutputStream}
import java.net.ServerSocket
import java.util.{Calendar, Date}

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.util.Random
import scala.util.control.Breaks._



/**
 * @author ${user.name}
 */
object ApacheLogGenerator {

  private val MAX_FILE_LINES: Int = 100
  private val N_COUNTRIES: Int = 20
  private val MAX_IPS_PER_COUNTRY: Int = 1000
  //private val SESSION_TIME_IN_SEC: Int = 900
  private val MAX_CLICKS_PER_USER: Int = 20

  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {

    val c: Calendar = Calendar.getInstance
    c.setTime(new Date)
    c.add(Calendar.DATE, 1)
    val d = c.getTime

    val n_records_per_day = args(0)
    val output = args(1)

    val logDataFeedActor = new ApacheLogGenerator
    logDataFeedActor.initialize
    logDataFeedActor.LogGenerator(d, n_records_per_day.toInt, output)
  }


}


class ApacheLogGenerator {

  val str: String = "Hello, How are you"
  private val ipA_by_ctry = Array.ofDim[Int](ApacheLogGenerator.MAX_IPS_PER_COUNTRY,ApacheLogGenerator.N_COUNTRIES)
  private val ipB_by_ctry = Array.ofDim[Int](ApacheLogGenerator.MAX_IPS_PER_COUNTRY,ApacheLogGenerator.N_COUNTRIES)
  private val tot_ips_by_ctry: Array[Int] = new Array[Int](ApacheLogGenerator.N_COUNTRIES)
  private val cum_hourly_weight_by_ctry = Array.ofDim[Int](24,ApacheLogGenerator.N_COUNTRIES)
  private val tot_weight_per_hour: Array[Int] = new Array[Int](24)
  private var tot_weight_per_day = 0


  private val ctry_ind: HashMap[String, String] = new HashMap[String, String]
  private var n_requests: Int = 0
  private var n_referrers: Int = 0
  private var n_user_agents: Int = 0

  private val requests: Array[String] = new Array[String](ApacheLogGenerator.MAX_FILE_LINES)
  private val referrers: Array[String] = new Array[String](ApacheLogGenerator.MAX_FILE_LINES)
  private val user_agents: Array[String] = new Array[String](ApacheLogGenerator.MAX_FILE_LINES)


  def initialize: Unit = {

    println("Begin Intialization")
    var ctry_abbr: Array[String] = Array[String]("CH", "US", "IN", "JP", "BR", "DE", "RU", "ID", "GB", "FR", "NG", "MX", "KR", "IR", "TR", "IT", "PH", "VN", "ES", "PK")
    for (i <- 0 to ApacheLogGenerator.N_COUNTRIES - 1) {
      tot_ips_by_ctry(i) = 0
      ctry_ind.put(ctry_abbr(i), String.valueOf(i))
    }

    for (i <- 0 to ApacheLogGenerator.MAX_IPS_PER_COUNTRY - 1) {
      for (j <- 0 to ApacheLogGenerator.N_COUNTRIES - 1) {
        ipA_by_ctry(i)(j) = 0
        ipB_by_ctry(i)(j) = 0
      }
    }

    initCountryIP
    initRequest
    initReferrers
    initUserAgents
    weight
    println("End Intialization")
  }

  def initCountryIP: Unit = {
    for (line <- Source.fromInputStream(getClass.getResourceAsStream("/all_classbs.txt")).getLines()) {
      //println("Line: " + line)
      val fields = line.split(" ")
      if (fields.length == 2) {
        //println("field(0): " + fields(0) + " fields(1) " + fields(1))
        val i_ctry_s: String = ctry_ind.getOrElse(fields(1), null)
        if (i_ctry_s != null) {
          val i_ctry = i_ctry_s.toInt
          val octets: Array[String] = fields(0).split("\\.")
          if (tot_ips_by_ctry(i_ctry) < ApacheLogGenerator.MAX_IPS_PER_COUNTRY) {
            ipA_by_ctry(tot_ips_by_ctry(i_ctry))(i_ctry) = octets(0).toInt
            ipB_by_ctry(tot_ips_by_ctry(i_ctry))(i_ctry) = octets(1).toInt
            tot_ips_by_ctry(i_ctry) += 1
          }
        }
      }
    }
  }

  def initRequest: Unit = {
    println("Begin initRequest")
    var i = 0
    for (line <- Source.fromInputStream(getClass.getResourceAsStream("/requests.txt")).getLines()) {
      requests(i) = line
      i += 1
    }
    n_requests = i

    println("Lines read= " + n_requests);
    for (i <- 0 to n_requests -1)
      println("Requests line " + i + ": " + requests(i));

    println("End initRequest")
  }

  def initReferrers: Unit ={
    var i = 0
    for (line <- Source.fromInputStream(getClass.getResourceAsStream("/referrers.txt")).getLines()) {
      referrers(i) = line
      i += 1
    }
    n_referrers = i

    println("Lines read= " + n_requests);
    for (i <- 0 to n_requests -1)
      println("Referrers line " + i + ": " + requests(i));

  }

  def initUserAgents: Unit ={
    var i = 0
    for (line <- Source.fromInputStream(getClass.getResourceAsStream("/user_agents.txt")).getLines()) {
      user_agents(i) = line
      i += 1
    }
    n_user_agents = i

    println("Lines read= " + n_requests);
    for (i <- 0 to n_requests -1)
      println("User agents line " + i + ": " + requests(i));

  }

  def weight: Unit= {

    var ctry_pct: Array[Int] = Array[Int](31, 13, 7, 5, 5, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 1, 1, 1)
    var hourly_weight: Array[Int] = Array[Int](4, 3, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 2, 2, 3, 4, 6, 8, 12, 12, 12, 10)
    var ctry_time_diff: Array[Int] = Array[Int](13, 0, 11, 14, 2, 7, 9, 12, 6, 7, 6, 0, 14, 10, 8, 7, 13, 12, 7, 10)
    var hourly_weight_by_ctry = Array.ofDim[Int](24,ApacheLogGenerator.N_COUNTRIES)


    for( hour <- 0 to 23){
      for(ctry <- 0 to ApacheLogGenerator.N_COUNTRIES - 1){
        val local_hour = (hour + ctry_time_diff(ctry)) % 24
        hourly_weight_by_ctry(hour)(ctry) = hourly_weight(local_hour) * ctry_pct(ctry)
      }
    }

    for( hour <- 0 to 23){
      var sum = 0
      for(ctry <- 0 to ApacheLogGenerator.N_COUNTRIES - 1){
        sum += hourly_weight_by_ctry(hour)(ctry)
        cum_hourly_weight_by_ctry(hour)(ctry) = sum
      }
      tot_weight_per_hour(hour) = sum
      tot_weight_per_day += sum;
    }
  }

  def LogGenerator(d: Date, n_records_per_day: Int, outputPath: String): Unit = {
    val out: DataOutputStream = new DataOutputStream(new FileOutputStream(outputPath))
    //val n_records_per_day: Int = 5000000
    val curr: Date = new Date
    var n_clicks_per_hour: Array[Int] = new Array[Int](24)
    var avg_time_between_clicks: Array[Double] = new Array[Double](24)
    var status: Array[Int] = Array[Int](200, 200, 200, 200, 200, 200, 200, 400, 404, 500)

    for(hour <- 0 to 23) {
      n_clicks_per_hour(hour) = Math.max(1, Math.floor(0.5 + n_records_per_day.toDouble *
                                               (tot_weight_per_hour(hour).toDouble)/tot_weight_per_day.toDouble).toInt)
      avg_time_between_clicks(hour) = (3600.toDouble / n_clicks_per_hour(hour)) * 1000
      System.out.println(" clicks: " + n_clicks_per_hour(hour) + " clickstime: " + avg_time_between_clicks(hour))
    }

    val rand: Random = new Random(curr.getTime)

    val time_of_day_in_sec: Double = 0.0
    val hour: Int = 0
    var clicks_left: Int = 0
    //var ip4: String = ""
    var referrer: String = ""
    var user_agent: String = ""

    var month_abbr: Array[String] = Array[String]("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

    while(true) {
      val currdate: Date = new Date
      if (currdate.getTime > d.getTime) break //todo: break is not supported
      val h: Int = currdate.getHours

      val delay: Long = avg_time_between_clicks(h).toLong
      //println("h: " + h + " delay: " + delay)
      Thread.sleep(delay)
      val day: Int = currdate.getDate
      val month: Int = currdate.getMonth
      val year: Int = currdate.getYear + 1900

      // Pick random number for given hour, then look up country in cum weights, then pick random row for IP
      val r = 1 + rand.nextInt(tot_weight_per_hour(hour))
      //println("tot_weight_per_hour: '" + tot_weight_per_hour(hour) + " hour: " + hour + "r: " + r)
      var ctry = 0
      while (r > cum_hourly_weight_by_ctry(hour)(ctry)) {
        ctry += 1; ctry
      }

      //println("tot_ips_by_ctry: '" + tot_ips_by_ctry(ctry) + " ctry: " + ctry)
      val i = rand.nextInt(tot_ips_by_ctry(ctry))
      //println("tot_ips_by_ctry: '" + tot_ips_by_ctry(ctry) + " ctry: " + ctry + "i: " + i)

      var ipv4: String = "%d.%d.%d.%d".format(ipA_by_ctry(i)(ctry), ipB_by_ctry(i)(ctry), 2 + rand.nextInt(249), 2 + rand.nextInt(249))
      //var ip4 = String.format("%d.%d.%d.%d", ipA_by_ctry(i)(ctry), ipB_by_ctry(i)(ctry), 2 + rand.nextInt(249), 2 + rand.nextInt(249))
      clicks_left = 1 + rand.nextInt(ApacheLogGenerator.MAX_CLICKS_PER_USER)
      referrer = referrers(rand.nextInt(n_referrers))
      user_agent = user_agents(rand.nextInt(n_user_agents))

      val timestamp: String =  "%02d:%02d:%02d".format(currdate.getHours, currdate.getMinutes, currdate.getSeconds)
      val output: String = "%s - %d [%02d/%3s/%4d:%8s -0500] \"%s\" %d %d \"%s\" \"%s\"\n".format( ipv4, i, day, month_abbr(month), year, timestamp, requests(rand.nextInt(n_requests)), status(rand.nextInt(10)), rand.nextInt(4096), referrer, user_agent)
      out.writeBytes(output)
      //kafka ! KafkaMessageEnvelope[String, String](KafkaTopic, KafkaKey, output)
    }
  }
}