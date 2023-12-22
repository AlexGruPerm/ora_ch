package app

import com.clickhouse.jdbc.{ClickHouseConnection, ClickHouseDataSource}
import ora.OraToCh

import java.sql.ResultSet
import java.util.Properties
import oracle.ucp.jdbc.PoolDataSourceFactory

import java.util.concurrent.TimeUnit
import scala.collection.immutable.ListMap

/**
 * Config:
 * https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcConfig.java
 */
object MainApp extends App{

  sealed trait convType
  case object str extends convType
  case object num extends convType

  sealed trait CellType
  final case class StrType(s: String) extends CellType
  final case class IntType(i: Int) extends CellType
  final case class NumType(d :Double) extends CellType

  type rows = List[ListMap[String,Option[CellType]]]

  case class DataCell(name: String, value: Option[CellType])

  def getColumnWsDatatype(ColumnTypeName: String, Precision: Int, Scale: Int, ct: convType): String = {
    ct match {
      case _: num.type => if (ColumnTypeName == "NUMBER") {
        if (Precision > 0 && Scale == 0) "INTEGER"
        else "DOUBLE"
      } else "STRING"
      case _: str.type => "STRING"
    }
  }

  def isNumInString(s: String) = {
    if (s!=null && s.nonEmpty)
      s.replace(".", "").replace(",", "").replace("-","")
        .forall(_.isDigit)
    else
      false
  }

  def seqCellToMap(sc: IndexedSeq[DataCell]): ListMap[String, Option[CellType]] =
    sc.foldLeft(ListMap.empty[String, Option[CellType]]) {
      case (acc, pr) => acc + (pr.name -> pr.value)
    }

  def getRowsFromResultSet(rs: ResultSet, ct: convType): rows = {
    //val beginTs: Long = System.currentTimeMillis()
    val columns: IndexedSeq[(String, String)] = (1 to rs.getMetaData.getColumnCount)
      .map(cnum => (rs.getMetaData.getColumnName(cnum).toLowerCase,
        getColumnWsDatatype(
          rs.getMetaData.getColumnTypeName(cnum),
          rs.getMetaData.getPrecision(cnum),
          rs.getMetaData.getScale(cnum),
          ct
        )))

    /**
     * todo: remove debug output
     * columns.foreach(println)
     * val afterColumns: Long = System.currentTimeMillis()
     */

    rs.setFetchSize(1000)
    rs.setFetchDirection(ResultSet.TYPE_FORWARD_ONLY)

    /**
     * todo: remove debug output
     * println(s"FetchSize = ${rs.getFetchSize}")
     * println(s"FetchDirection = ${rs.getFetchDirection}")
     */

    val rDs = Iterator.continually(rs).takeWhile(_.next()).map { rs =>
      columns.map {
        cname =>
          DataCell(
            cname._1,
            {
              val cellValue: CellType =
                cname._2 match {
                  case "INTEGER" => IntType(rs.getInt(cname._1))
                  case "DOUBLE" => {
                    val d: Double = rs.getDouble(cname._1)
                    if (d % 1 == 0.0)
                      IntType(d.toInt)
                    else
                      NumType(d)
                  }
                  case _ =>
                    val s: String = rs.getString(cname._1)
                    ct match {
                      case _: num.type =>
                        if (isNumInString(s)) {
                          val d: Double = s.replace(",", ".").toDouble
                          if (d % 1 == 0.0)
                            IntType(d.toInt)
                          else
                            NumType(d)
                        } else
                          StrType(s)
                      case _: str.type => StrType(s)
                    }
                }
              if (rs.wasNull()) None else Some(cellValue)
            }
          )
      }
    }.toList.map(sc => seqCellToMap(sc))

    //val finishTs: Long = System.currentTimeMillis()

    /**
     * todo: remove debug output
     * println(s"Fetched rows : ${rDs.size}")
     * println("Columns types(sec): " + TimeUnit.MILLISECONDS.toSeconds(afterColumns-beginTs))
     * println("Fetching rows(sec): " + TimeUnit.MILLISECONDS.toSeconds(finishTs-afterColumns))
     * rDs.head.foreach(r => println(s" Cell meta - ${r._1} - ${r._2}"))
     */
    rDs
  }

  Console.println(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")
  //JdbcConfig
  val url = "jdbc:clickhouse:http://192.168.56.100:8123/data"


  /**
   * todo: TRY
   * httpCompression: "gzip"
   * enableHttpCompression: "true"
   * https://github.com/ClickHouse/clickhouse-java/issues/721
  */

  /** Connection Properties:
   * https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcConfig.java
   */
  val properties = new Properties()
  properties.setProperty("http_connection_provider", "HTTP_URL_CONNECTION")

  val dataSource = new ClickHouseDataSource(url, properties)

  val beforeClickhouseTs: Long = System.currentTimeMillis()
  val conn: ClickHouseConnection = dataSource.getConnection("admin", "admin")
  Console.println(s"Clickhouse connection isClosed = ${conn.isClosed}")

  //https://clickhouse.com/docs/en/integrations/java#with-input-table-function
  val stmt = conn.createStatement()
  val rs: ResultSet = stmt.executeQuery("select sum(1) as cnt from data.eaist_lot")

  val columns: List[(String, String)] = (1 to rs.getMetaData.getColumnCount)
    .map(cnum => (rs.getMetaData.getColumnName(cnum), rs.getMetaData.getColumnTypeName(cnum))).toList

  columns.foreach(println)

  val rows = Iterator.continually(rs).takeWhile(_.next()).map { rs =>
    columns.map(
      cname => rs.getString(cname._1)
    )
  }.toList.flatten

  val afterClickhouseTs: Long = System.currentTimeMillis()

  println(s"Clickhouse query duration : ${afterClickhouseTs-beforeClickhouseTs} ms.")

  rows.foreach(println)


  /**
   * note
   * Use PreparedStatement instead of Statement
   * Use input function whenever possible
  */

  Console.println(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")

  val pds = PoolDataSourceFactory.getPoolDataSource
  pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource")
  pds.setURL(s"jdbc:oracle:thin:@//PRM-WS-0371.MOON.LAN:1521/mts")
  pds.setUser("data")
  pds.setPassword("data")
  pds.setConnectionPoolName("Ucp")
  pds.setConnectionWaitTimeout(10)
  pds.setInitialPoolSize(1)
  // A connection pool always tries to return to the minimum pool size
  pds.setMinPoolSize(4)
  //The maximum pool size property specifies the maximum number of available
  // and borrowed (in use) connections that a pool maintains.
  pds.setMaxPoolSize(8)
  pds.setAbandonedConnectionTimeout(30)
  pds.setInactiveConnectionTimeout(60)
  //todo: check it.
  //https://docs.oracle.com/en/database/oracle/oracle-database/12.2/jjucp/validating-ucp-connections.html#GUID-A7C850D6-4026-4629-BCFA-9181C29EFBF9
  pds.setValidateConnectionOnBorrow(true)

  val jdbcVersion: String = {
    val conn: java.sql.Connection = pds.getConnection
    conn.setClientInfo("OCSID.MODULE", "WS")
    conn.setClientInfo("OCSID.ACTION", "META")
    val md: java.sql.DatabaseMetaData = conn.getMetaData
    val jdbcVers: String = s"${md.getJDBCMajorVersion}.${md.getJDBCMinorVersion}"
    conn.close()
    jdbcVers
  }

  println(s"Oracle jdbc version is ${jdbcVersion}")

  val oraConn = pds.getConnection()
  Console.println(s"Oracle connection isClosed = ${oraConn.isClosed}")

  oraConn.setClientInfo("OCSID.MODULE", "WS_CONN")
  oraConn.setClientInfo("OCSID.ACTION", "SELECT")

  val query: String = """ select id,
                        |        registrynumber,
                        |        tendersubject,
                        |        lotnumber,
                        |        lotprice
                        |  from EAIST_LOT_SS t """.stripMargin

  val beforeExecTs: Long = System.currentTimeMillis()
  val oraRs: ResultSet = oraConn.createStatement.executeQuery(query)
  oraRs.setFetchSize(10000)
  oraRs.setFetchDirection(ResultSet.TYPE_FORWARD_ONLY)

  val afterExecTs: Long = System.currentTimeMillis()
  //val oraRows = getRowsFromResultSet(oraRs, num)
  OraToCh.saveToClickhouse("data.eaist_lot", oraRs, conn, 20000)
  val afterFetchTs: Long = System.currentTimeMillis()

  //println(s"Execution time : ${afterExecTs - beforeExecTs} ms.")

  println(s"Save time: ${afterFetchTs - afterExecTs} ms.")
  //println(s"Fetched ${oraRows.size} rows.")

  Console.println(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")

}
