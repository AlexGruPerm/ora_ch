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
   *
   * maybe we need use query for varchar columns:
   *
   * select atc.COLUMN_NAME,atc.CHAR_USED
   * from  ALL_TAB_COLUMNS atc
   * where atc.OWNER='MSK_ANALYTICS' and
   * atc.TABLE_NAME='V_GP_KBK_UN' and
   * atc.COLUMN_NAME='CR_CODE' and
   * atc.DATA_TYPE='VARCHAR2'
   * order by atc.COLUMN_ID
   *
   * CHAR_USED can be B(Byte) or C(Char)
   *
   * Not works:
   * oraConn.getMetaData.getColumns(null,"msk_analytics","v_gp_kbk_un","cr_code").getInt("CHAR_OCTET_LENGTH")
   *
  */
  def recreateTableCopyData(table: Table, rs: ZIO[Any, Exception, ResultSet], batch_size: Int): ZIO[Any, Exception, Int] =
    for {
    fn <- ZIO.fiberId.map(_.threadName)
    _ <- ZIO.logInfo(s"recreateTableCopyData fiber name : $fn")
    oraRs <- rs

/*    _ <- ZIO.logInfo(s"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    _ <- ZIO.foreachDiscard((1 to oraRs.getMetaData.getColumnCount)){i =>
      ZIO.logInfo(
        s"""${oraRs.getMetaData.getColumnName(i).toLowerCase} -
           |${oraRs.getMetaData.getColumnTypeName(i)} -
           |${oraRs.getMetaData.getColumnClassName(i)} -
           |${oraRs.getMetaData.getColumnDisplaySize(i)} -
           |${oraRs.getMetaData.getPrecision(i)} -
           |${oraRs.getMetaData.getScale(i)}
           |""".stripMargin)
       }
    _ <- ZIO.logInfo(s"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")*/

    cols = (1 to oraRs.getMetaData.getColumnCount)
      .map(i =>
        OraChColumn(
          oraRs.getMetaData.getColumnName(i).toLowerCase,
          oraRs.getMetaData.getColumnTypeName(i),
          oraRs.getMetaData.getColumnClassName(i),
          oraRs.getMetaData.getColumnDisplaySize(i),
          /*todo:
             06X0100000 - 10 Chars as in getColumnDisplaySize, but 11 Bytes.
             where X is Cyrillic letter.
          */
          //todo: add this property to json. "col_precision_2x":["cr_code"]
          if (oraRs.getMetaData.getColumnName(i).toLowerCase == "cr_code")
              oraRs.getMetaData.getPrecision(i)*2
            else
              oraRs.getMetaData.getPrecision(i),
          oraRs.getMetaData.getScale(i),
          oraRs.getMetaData.isNullable(i),
          table.notnull_columns.getOrElse(List.empty[String])
        )).toList
    nakedCols = cols.map(chCol => chCol.name).mkString(",\n")
    colsScript = cols.map(chCol => chCol.clColumnString).mkString(",\n")

    _ <- ZIO.logInfo(s"keyType = ${table.keyType.toString} External pk_columns = ${table.pk_columns}")

    createScript =
      s"""create table ${table.schema}.${table.name}
         |(
         | $colsScript
         |) ENGINE = MergeTree()
         | ${
             table.partition_by match {
               case Some(part_by) => s"PARTITION BY ($part_by)"
               case None => " "
             }
            }
         | ${
              table.keyType match {
                case ExtPrimaryKey => s"PRIMARY KEY (${table.pk_columns.getOrElse(table.keyColumns)})"
                case PrimaryKey => s"PRIMARY KEY (${table.keyColumns})"
                case RnKey | UniqueKey  => s"ORDER BY (${table.keyColumns})"
              }
            }
         |""".stripMargin

    _ <- ZIO.logInfo(s"createScript = $createScript")

    insQuer =
      s"""insert into ${table.schema}.${table.name}
         |select $nakedCols
         |from
         |input('$colsScript
         |      ')
         |""".stripMargin

    _ <- ZIO.logInfo(s"insQuer = $insQuer")

    /**todo: replace with attemptBlockingCancelable or attemptBlockingInterrupt
    */
    rows <- ZIO.attemptBlockingInterrupt {
      sess.createStatement.executeQuery(s"drop table if exists ${table.schema}.${table.name}")
      sess.createStatement.executeQuery(createScript)

      //------------------------------------
      val ps: PreparedStatement = sess.prepareStatement(insQuer)

      Iterator.continually(oraRs).takeWhile(_.next()).foldLeft(1) {
        case (counter, rs) =>
          cols.foldLeft(1) {
            case (i, c) =>
              (c.typeName, c.scale) match {
                // Long - because getInt is that 4294967298 is outside the range of Java's int
                case ("NUMBER", 0) => ps.setLong(i, rs.getLong(c.name))
/*                  {
                    println(s"NUMBER_0 ${c.name}")
                    val intVal: Long = rs.getLong(c.name)
                    println(s"intVal = $intVal")
                    ps.setLong(i, intVal)
                    //ps.SetLong ???
                  }*/
                case ("NUMBER", _) => ps.setDouble(i, rs.getDouble(c.name))
/*                  {
                    println(s"NUMBER__ ${c.name}")
                    val dblVal: Double = rs.getDouble(c.name)
                    println(s"dblVal = $dblVal")
                    ps.setDouble(i, dblVal)
                  }*/
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
                    if (dateAsUnixtimestamp <= 0L)
                      ps.setObject(i, "1971-01-01 00:00:00")
                    else {
                      if (dateAsUnixtimestamp >= 4296677295L)
                        ps.setObject(i, "2106-01-01 00:00:00")
                      else
                        ps.setObject(i, tmp)
                    }
                  } else
                      ps.setNull(i,Types.DATE)
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

    }.ensuring(
      ZIO.logError("End of the blocking operation [attemptBlockingInterrupt] in recreateTableCopyData.")
    ).catchAll {
      case e: Exception =>
        ZIO.logError(s"${e.getMessage} - ${e.getCause} - ${e.getStackTrace.mkString("Array(", ", ", ")")}") *>
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
    _ <- ZIO.logInfo("~~~~~~~~~~~~~~~ Clickhouse connect properties ~~~~~~~~~~~~~~~~")
    cnt = props.keySet().size()
    _ <- ZIO.logInfo(s"Connection has $cnt properties")
    keys = props.keySet().toArray.map(_.toString).toList
    _ <- ZIO.foreachDiscard(keys)(k => ZIO.logInfo(s"${k} - ${props.getProperty(k)}]"))
    _ <- ZIO.logInfo("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    _ <- ZIO.logInfo(s" = [${props.getProperty("")}]")
  } yield session

  override def chConnection(taskId: Int): ZIO[Any, Exception, chSess] = for {
    _ <- ZIO.unit
    sessEffect = ZIO.attemptBlocking {
      props.setProperty("http_connection_provider", "HTTP_URL_CONNECTION")
      /*
      props.setProperty("http_connection_provider", "APACHE_HTTP_CLIENT")
      props.setProperty("socket_keepalive", "true")
      props.setProperty("socket_timeout", "100000000") //ms.
      props.setProperty("max_execution_time", "100000000")
      props.setProperty("connection_timeout", "100000000")
      props.setProperty("dataTransferTimeout", "100000000")
      props.setProperty("timeToLiveMillis", "100000000")
      */

      /*
      set max_execution_time=60000;
      clickHouseDataSource.getProperties().setConnectionTimeout(10000000);
      clickHouseDataSource.getProperties().setSocketTimeout(10000000);
      clickHouseDataSource.getProperties().setTimeToLiveMillis(10000000);
      clickHouseDataSource.getProperties().setSessionTimeout(10000000L);
      clickHouseDataSource.getProperties().setDataTransferTimeout(10000000);
      clickHouseDataSource.getProperties().setMaxExecutionTime(10000000);
      */
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