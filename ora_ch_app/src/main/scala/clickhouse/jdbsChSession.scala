package clickhouse

import column.OraChColumn
import com.clickhouse.client.config.ClickHouseClientOption
import com.clickhouse.client.http.config.HttpConnectionProvider
import com.clickhouse.jdbc.{ClickHouseConnection, ClickHouseDataSource, ClickHouseDriver}
import conf.ClickhouseServer
import table._
import zio.{ZIO, ZLayer}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Types}
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Properties

case class chSess(sess : Connection, taskId: Int){

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def dateTimeStringToEpoch(s: String): Long =
    LocalDateTime.parse(s, formatter).toEpochSecond(ZoneOffset.UTC)

  /**
   * return count of copied rows.
   */
  def getCountCopiedRows(table: Table): ZIO[Any, Nothing, Int] = for {
    rows <- ZIO.attempt {
      val rsRowCount = sess.createStatement.executeQuery(s"select count() as cnt from ${table.schema}.${table.name}")
      rsRowCount.next()
      val rowCount = rsRowCount.getInt(1)
      rowCount
      }.catchAllDefect {
        case e: Exception => ZIO.logError(s"No problem. getCountCopiedRows - Defect - ${e.getMessage}") *>
          ZIO.succeed(0)
      }.catchAll {
      case e: Exception => ZIO.logError(s"No problem. getCountCopiedRows - Exception - ${e.getMessage}") *>
        ZIO.succeed(0)
    }
  } yield rows


  /**
   * return count of copied rows.
  */
  def recreateTableCopyData(table: Table, rs: ZIO[Any, Exception, ResultSet], batch_size: Int): ZIO[Any, Exception, Int] = for {
    oraRs <- rs
    cols = (1 to oraRs.getMetaData.getColumnCount)
      .map(i =>
        OraChColumn(
          oraRs.getMetaData.getColumnName(i).toLowerCase,
          oraRs.getMetaData.getColumnTypeName(i),
          oraRs.getMetaData.getColumnClassName(i),
          oraRs.getMetaData.getColumnDisplaySize(i),
          oraRs.getMetaData.getPrecision(i),
          oraRs.getMetaData.getScale(i),
          oraRs.getMetaData.isNullable(i)
        )).toList
    nakedCols = cols.map(chCol => chCol.name).mkString(",\n")
    colsScript = cols.map(chCol => chCol.clColumnString).mkString(",\n")
    createScript =
      s"""create table ${table.schema}.${table.name}
         |(
         | $colsScript
         |) ENGINE = MergeTree()
         | ${
              table.keyType match {
                case PrimaryKey => s"PRIMARY KEY (${table.keyColumns})"
                case RnKey | UniqueKey  => s"ORDER BY (${table.keyColumns})"
              }
            }
         |""".stripMargin

    insQuer =
      s"""insert into ${table.schema}.${table.name}
         |select $nakedCols
         |from
         |input('$colsScript
         |      ')
         |""".stripMargin

    //_ <- ZIO.logInfo(s"batch_size = $batch_size")
    //_ <- ZIO.logInfo(insQuer)

    rows <- ZIO.attemptBlocking {
      sess.createStatement.executeQuery(s"drop table if exists ${table.schema}.${table.name}")
      sess.createStatement.executeQuery(createScript)

      //------------------------------------
      val ps: PreparedStatement = sess.prepareStatement(insQuer)

      Iterator.continually(oraRs).takeWhile(_.next()).foldLeft(1) {
        case (counter, rs) =>
          cols.foldLeft(1) {
            case (i, c) =>
              (c.typeName, c.scale) match {
                case ("NUMBER", 0) => ps.setInt(i, rs.getInt(c.name))
                case ("NUMBER", _) => ps.setDouble(i, rs.getDouble(c.name))
                /*
                  {
                  val d: java.math.BigDecimal = rs.getBigDecimal(c.name)
                  println(s"BigDecimal value = $d")
                  ps.setBigDecimal(i,d)
                }
                */
                case ("CLOB", _) => ps.setString(i, rs.getString(c.name))
                case ("VARCHAR2", _) => ps.setString(i, rs.getString(c.name))
                case ("DATE", _) => {
                  val tmp = rs.getString(c.name)
                  val isNull: Boolean = rs.wasNull()
                  if (!isNull) {
                    val dateAsUnixtimestamp = dateTimeStringToEpoch(tmp)
                    if (dateAsUnixtimestamp < -18000L)
                      ps.setObject(i, "1970-01-01 00:00:00")
                    else {
                      if (dateAsUnixtimestamp > 4296677295L)
                        ps.setObject(i, "2106-01-01 00:00:00")
                      else
                        ps.setObject(i, tmp)
                    }
                  } else
                    ps.setObject(i, Types.NULL) //todo: compare with ps.setNull(i,Types.DATE)
                }
              }
              i + 1
          }

          ps.addBatch()
          if (counter == batch_size) {
            ps.executeBatch()
            0
          } else
            counter + 1
      }
      ps.executeBatch()

      val rsRowCount = sess.createStatement.executeQuery(s"select sum(1) as cnt from ${table.schema}.${table.name}")
      rsRowCount.next()
      val rowCount = rsRowCount.getInt(1)
      rowCount

    }.catchAll {
      case e: Exception => ZIO.logError(e.getMessage) *>
        ZIO.fail(new Exception(s"${e.getMessage}"))
    }

    _ <- ZIO.logInfo(s"Copied $rows rows to ${table.schema}.${table.name}")

  } yield rows



  def createDatabases(schemas: Set[String]): ZIO[Any, Exception, Unit] = for {
    _ <- ZIO.unit
    createDbEffs = schemas.map {schema =>
      ZIO.attemptBlocking {
        val query: String =
          s"CREATE DATABASE IF NOT EXISTS $schema"
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        rs.next()
      }.catchAll {
        case e: Exception => ZIO.logError(e.getMessage) *>
          ZIO.fail(new Exception(s"${e.getMessage}"))
      }
    }
    _ <- ZIO.collectAll(createDbEffs)
  } yield ()

}

trait jdbcChSession {
  def sess(taskId: Int): ZIO[Any,Exception,chSess]
  val props = new Properties()
  def chConnection(taskId: Int): ZIO[Any,Exception,chSess]
}

case class jdbcSessionImpl(ch: ClickhouseServer) extends jdbcChSession {

  def sess(taskId: Int): ZIO[Any, Exception, chSess] = for {
    session <- chConnection(taskId)
  } yield session

  override def chConnection(taskId: Int): ZIO[Any, Exception, chSess] = for {
    _ <- ZIO.unit
    sessEffect = ZIO.attemptBlocking {
      //ClickHouseClientOption
      //HttpConnectionProvider

      props.setProperty("http_connection_provider", "APACHE_HTTP_CLIENT"/*"HTTP_CLIENT"*//*"HTTP_URL_CONNECTION"*/)
      val dataSource = new ClickHouseDataSource(ch.getUrl, props)
      val conn: ClickHouseConnection = dataSource.getConnection(ch.user, ch.password)


      /*
      DriverManager.registerDriver(new ClickHouseDriver)
      val properties = new Properties()
      properties.setProperty("user", ch.user);
      properties.setProperty("password", ch.password);
      val conn = DriverManager.getConnection(ch.getUrl, properties)
      */

      chSess(conn,taskId)
    }.catchAll {
      case e: Exception => ZIO.logError(s"${e.getMessage} - url=[${ch.getUrl}] user=[${ch.user}] password=[${ch.password}] port=[${ch.port}] provider = [${props.getProperty("http_connection_provider")}]") *>
        ZIO.fail(new Exception(s"${e.getMessage} - url=[${ch.getUrl}] user=[${ch.user}] password=[${ch.password}] port=[${ch.port}] provider = [${props.getProperty("http_connection_provider")}]"))
    }
    _ <- ZIO.logInfo(s"  ") *>
      ZIO.logInfo(s"New clickhouse connection =============== >>>>>>>>>>>>> ")
    sess <- sessEffect

  } yield sess

}

object jdbcChSessionImpl {

  val layer: ZLayer[ClickhouseServer, Exception, jdbcChSession] =
    ZLayer{
      for {
        chs <- ZIO.service[ClickhouseServer]
      } yield jdbcSessionImpl(chs)
    }

}