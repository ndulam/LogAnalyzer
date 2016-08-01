package com.data.cassandra.dao


import com.datastax.driver.core._
import com.datastax.driver.core.policies.{ConstantReconnectionPolicy, DowngradingConsistencyRetryPolicy}
import org.slf4j.{LoggerFactory, Logger}


object  LogEventDAO {

  val logger: Logger = LoggerFactory.getLogger(classOf[LogEventDAO])
}
class LogEventDAO extends Serializable {

  var session: Session = null
  var cluster: Cluster = null
  var keyspace: String = null


  def init(contactNode: String) = {
    cluster = Cluster.builder.addContactPoint(contactNode).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE).withReconnectionPolicy(new ConstantReconnectionPolicy(100L)).build

    val metadata: Metadata = cluster.getMetadata
    LogEventDAO.logger.info("Connected to cluster: %s\n", metadata.getClusterName)
    import scala.collection.JavaConversions._
    for (host <- metadata.getAllHosts) {
      LogEventDAO.logger.info("DataCenter: %s, Host: %s, Rack: %s\n", host.getDatacenter, host.getAddress, host.getRack)
    }

    this.connect()
  }

  def init(contactNodes: Seq[String], key_space: String) = {

    for (node <- contactNodes) println(node)

    keyspace = key_space
    cluster = Cluster.builder.addContactPoints(contactNodes:_*).withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE).withReconnectionPolicy(new ConstantReconnectionPolicy(100L)).build()

    val metadata: Metadata = cluster.getMetadata

    LogEventDAO.logger.debug("Connected to cluster: {}", metadata.getClusterName)

    import scala.collection.JavaConversions._

    for (host <- metadata.getAllHosts) {

      LogEventDAO.logger.info("DataCenter: %s, Host: %s, Rack: %s\n", host.getDatacenter, host.getAddress, host.getRack)
    }

    this.connect()
  }

  /**
   * Connects to the cassandra cluster using the provided keyspace
   * @param keyspace
   */
  def connect(keyspace: String): Unit =  {
    if (session == null) {
      session = cluster.connect(keyspace);
    }
  }

  def connect(): Unit =  {
    if (session == null) {
      session = cluster.connect();
    }
  }

  /**
   * Returns a session instance of the connection
   * @return session instance
   */
  def getSession(): Session =  this.session


  /**
   * Disconnects from cassandra cluster
   */
  def close(): Unit = {
    if (session != null) {
      session.close
      LogEventDAO.logger.debug("Successfully closed session")
    }
    if (cluster != null) {
      cluster.close
      LogEventDAO.logger.debug("Successfully closed cluster connection")
    }
  }


  /**
   * Creates required schema for movie dataset with provided keyspace name
   * @param repFactor replication factor for the keyspace
   */
  def createSchema(repFactor: Integer): Unit =  {
    getSession().execute("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': %d};".format(keyspace, repFactor))
    getSession.execute("CREATE TABLE IF NOT EXISTS %s.page_views (page VARCHAR, views COUNTER, PRIMARY KEY(page));".format(keyspace))
    getSession.execute("CREATE TABLE IF NOT EXISTS %s.log_volume_by_minute (timestamp VARCHAR, count COUNTER, PRIMARY KEY(timestamp));".format(keyspace))
    getSession.execute("CREATE TABLE IF NOT EXISTS %s.status_counter (status_code INT, count COUNTER, PRIMARY KEY(status_code));".format(keyspace))
    //getSession.execute("CREATE TABLE IF NOT EXISTS %s.visits_by_country (country VARCHAR, city VARCHAR, count COUNTER, PRIMARY KEY(country, city));".format(keyspace))
    getSession.execute("CREATE TABLE IF NOT EXISTS %s.visits_by_country (country VARCHAR, count COUNTER, PRIMARY KEY(country));".format(keyspace))
  }

  def updatePageViews(page_url: String, count: Integer) {
    getSession.execute("UPDATE %s.page_views SET views = views + %d WHERE page='%s'".format(keyspace, count, page_url))
  }

  def updateLogVolumeByMinute(timestamp: String, count: Integer) {
    getSession.execute("UPDATE %s.log_volume_by_minute SET count = count + %d WHERE timestamp='%s'".format(keyspace, count, timestamp))
  }

  def updateStatusCounter(status_code: Integer, count: Long) {
    getSession.execute("UPDATE %s.status_counter SET count = count + %d WHERE status_code=%d".format(keyspace, count, status_code))
  }

  /*def updateVisitsByCountry(country: String, city: String, count: Integer) {
    getSession.execute("UPDATE %s.visits_by_country SET count = count + %d WHERE country='%s' AND city='%s'".format(keyspace, count, country, city))
  }*/

  def updateVisitsByCountry(country: String, count: Integer) {
    getSession.execute("UPDATE %s.visits_by_country SET count = count + %d WHERE country='%s'".format(keyspace, count, country))
  }

  def findCQLByQuery(CQL: String) {
    val cqlQuery: Statement = new SimpleStatement(CQL)
    cqlQuery.setConsistencyLevel(ConsistencyLevel.ONE)
    cqlQuery.enableTracing
    val resultSet: ResultSet = getSession.execute(cqlQuery)
    val columnDefinitions: ColumnDefinitions = resultSet.getColumnDefinitions
    import scala.collection.JavaConversions._
    for (row <- resultSet) {
      LogEventDAO.logger.debug(row.toString)
    }
  }

  /**
   * Drops the specified keyspace
   */
  def dropSchema {
    getSession.execute("DROP KEYSPACE " + keyspace)
    LogEventDAO.logger.info("Finished dropping " + keyspace + " keyspace.")
  }

  /**
   * Drops a specified table from a given keyspace
   * @param table to delete
   */
  def dropTable(table: String) {
    getSession.execute("DROP TABLE IF EXISTS " + keyspace + "." + table)
    LogEventDAO.logger.info("Finished dropping table " + table + " from " + keyspace + " keyspace.")
  }

}