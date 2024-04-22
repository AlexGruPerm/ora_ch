package clickhouse

import calc.{ CalcParams, ViewQueryMeta }
import com.clickhouse.client.config.ClickHouseClientOption
import com.clickhouse.client.http.config.HttpConnectionProvider
import com.clickhouse.jdbc.{ ClickHouseConnection, ClickHouseDataSource, ClickHouseDriver }
import common.{ AppendRowsWQuery, SessTypeEnum, UpdateStructsScripts }
import conf.ClickhouseServer
import table._
import zio.{ Clock, Schedule, ZIO, ZLayer }

import java.sql.{ Connection, DriverManager, PreparedStatement, ResultSet, SQLException, Types }
import java.time.{ LocalDateTime, ZoneOffset }
import java.time.format.DateTimeFormatter
import java.util.{ Calendar, Properties }
import common.Types._
import request.AppendWhere

import java.util.concurrent.TimeUnit
import scala.annotation.nowarn

case class chSess(sess: Connection, taskId: Int) {

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  val chAmdinPass: String = "admin"

  def dateTimeStringToEpoch(s: String): Long =
    LocalDateTime.parse(s, formatter).toEpochSecond(ZoneOffset.UTC)

  def isTableExistsCh(table: Table): ZIO[Any, SQLException, Int] = for {
    tblExists <- ZIO.attemptBlockingInterrupt {
                   val rs       = sess
                     .createStatement
                     .executeQuery(
                       s"""
                          | select if(exists(
                          |				          select 1
                          |				            from system.tables t
                          |				           where t.database = '${table.schema.toLowerCase}' and
                          |				                 t.name     = '${table.name.toLowerCase}'
                          |                 ),1,0) as is_table_exists
                          |""".stripMargin
                     )
                   rs.next()
                   val resTblEx = rs.getInt(1)
                   rs.close()
                   resTblEx
                 }.tapError(er => ZIO.logError(er.getMessage))
                   .refineToOrDie[SQLException]
  } yield tblExists

  def updateMergeTree(
    table: Table,
    primaryKeyColumnsCh: List[String]
  ): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           val pkColsStr      = primaryKeyColumnsCh.mkString(",")
           val updateDictName = s"dict_${table.name}"
           val updateColsSql  = table
             .update_fields
             .getOrElse("empty_update_fields")
             .split(",")
             .map { col =>
               s"$col = dictGet(${table.schema}.$updateDictName, '$col', ($pkColsStr))"
             }
             .mkString(",")
           val updateQuery    =
             s"""
                |ALTER TABLE ${table.schema}.${table.name}
                |UPDATE
                |       $updateColsSql
                |where dictHas('${table.schema}.$updateDictName', ($pkColsStr));
                |""".stripMargin

           val rs = sess.createStatement.executeQuery(updateQuery)
           rs.close()
           updateQuery
         }.tapBoth(
           er => ZIO.logError(s"updateMergeTree ERROR - ${er.getMessage}"),
           update => ZIO.logDebug(s"updateColumns query = $update")
         ).refineToOrDie[SQLException]
  } yield ()

  def getMaxValueByColCh(table: Table): ZIO[Any, SQLException, MaxValAndCnt] = for {
    maxVal <- ZIO.attemptBlockingInterrupt {

                /**
                 * We have 2 append mode: 1) sync_by_column_max - single column In this case we need
                 * to know current maxVal. 2) sync_by_columns - by multiple fields. In this case
                 * don't need maxVal
                 */
                val q =
                  s""" select
                     |        ${table.sync_by_column_max orElse table.sync_update_by_column_max match {
                      case Some(syncSingleColumn) => s"max($syncSingleColumn)"
                      case None                   => " 0 "
                    }}
                     |        as maxVal,sum(1) as cnt
                     |   from ${table.schema.toLowerCase}.${table.name.toLowerCase} """.stripMargin

                val rs  = sess.createStatement.executeQuery(q)
                rs.next()
                val mvc = MaxValAndCnt(rs.getLong(1), rs.getLong(2))
                mvc
              }.tapError(er => ZIO.logError(er.getMessage))
                .refineToOrDie[SQLException]
  } yield maxVal

  def getMaxColForSync(table: Table): ZIO[Any, SQLException, Option[MaxValAndCnt]] = for {
    tblExistsCh <- isTableExistsCh(table).when(table.recreate == 0)
    maxColCh    <- getMaxValueByColCh(table).when(tblExistsCh.getOrElse(0) == 1)
  } yield maxColCh

  private def getSyncWhereFilterRsTuples(table: Table): ZIO[Any, SQLException, List[Any]] = for {
    res <- ZIO.attemptBlockingInterrupt {
             val q  =
               s""" select distinct ${table.sync_by_columns.getOrElse(" CH_EMPTY_SYNC_COLUMNS ")}
                  |   from ${table.schema.toLowerCase}.${table.name.toLowerCase} """.stripMargin
             val rs = sess.createStatement.executeQuery(q)

             val filterTuples: List[Any] =
               table.syncArity() match {
                 case 1 =>
                   Iterator
                     .continually(rs)
                     .takeWhile(_.next())
                     .map { rs =>
                       rs.getLong(1)
                     }
                     .toList
                 case 2 =>
                   Iterator
                     .continually(rs)
                     .takeWhile(_.next())
                     .map { rs =>
                       (rs.getLong(1), rs.getLong(2))
                     }
                     .toList
                 case 3 =>
                   Iterator
                     .continually(rs)
                     .takeWhile(_.next())
                     .map { rs =>
                       (rs.getLong(1), rs.getLong(2), rs.getLong(3))
                     }
                     .toList
                 case _ => List.empty[Any]
               }
             filterTuples
           }.refineToOrDie[SQLException]
             .tapError(er =>
               ZIO.logError(s"Error in getSyncWhereFilterRsTuples - ${er.getMessage}")
             )

  } yield res

  def whereAppendInt1(table: Table): ZIO[Any, SQLException, Option[List[Long]]] = for {
    res <- getSyncWhereFilterRsTuples(table)
  } yield Some(res.map(_.asInstanceOf[Long]))

  def whereAppendInt2(table: Table): ZIO[Any, SQLException, Option[List[(Long, Long)]]] = for {
    res <- getSyncWhereFilterRsTuples(table)
  } yield Some(res.map(_.asInstanceOf[(Long, Long)]))

  def whereAppendInt3(table: Table): ZIO[Any, SQLException, Option[List[(Long, Long, Long)]]] =
    for {
      res <- getSyncWhereFilterRsTuples(table)
    } yield Some(res.map(_.asInstanceOf[(Long, Long, Long)]))

  /**
   * Return the list of Primary key columns for clickhouse table. Using in part:) Engine = Join(ANY,
   * LEFT, date_start,date_end,id);
   *
   * select t.primary_key from system.tables t where t.database='msk_arm_v2' and t.name='evc'
   */
  def getPkColumns(table: Table): ZIO[Any, SQLException, List[String]] = for {
    pkString <- ZIO.attemptBlockingInterrupt {
                  val rs  = sess
                    .createStatement
                    .executeQuery(s"""
                                     | select t.primary_key
                                     |    from system.tables t
                                     |    where t.database = '${table.schema.toLowerCase}' and
                                     |    t.name           = '${table.name.toLowerCase}'
                                     |""".stripMargin)
                  rs.next()
                  val pks = rs.getString(1)
                  rs.close()
                  pks
                }.tapError(er => ZIO.logError(er.getMessage))
                  .refineToOrDie[SQLException]
  } yield pkString.split(",").toList

  /**
   * return count of copied rows.
   */
  def getCountCopiedRows(table: Table): ZIO[Any, Nothing /*SQLException*/, Long] = for {
    rows <- ZIO.attempt {
              val rsRowCount = sess
                .createStatement
                .executeQuery(s""" select t.total_rows
                                 | from system.tables t
                                 | where t.database = '${table.schema}' and
                                 |       t.name      = '${table.name}' """.stripMargin)
              rsRowCount.next()
              val rowCount   = rsRowCount.getLong(1)
              rowCount
            }.tapError(er => ZIO.logError(s"ERROR - getCountCopiedRows : ${er.getMessage}"))
              .tapDefect(df => ZIO.logError(s"DEFECT - getCountCopiedRows : ${df.toString}")) orElse
              ZIO.succeed(0L)
    // .refineToOrDie[SQLException]
    // .refineToOrDie[SQLException] orElse ZIO.succeed(0L)
  } yield rows

  /*  private def debugRsColumns(rs: ResultSet): ZIO[Any, Nothing, Unit] = for {
    _ <- ZIO.foreachDiscard(1 to rs.getMetaData.getColumnCount) { i =>
           ZIO.logTrace(s"""${rs.getMetaData.getColumnName(i).toLowerCase} -
                           |${rs.getMetaData.getColumnTypeName(i)} -
                           |${rs.getMetaData.getColumnClassName(i)} -
                           |${rs.getMetaData.getColumnDisplaySize(i)} -
                           |${rs.getMetaData.getPrecision(i)} -
                           |${rs.getMetaData.getScale(i)}
                           |""".stripMargin)
         }
  } yield ()*/

  def deleteRowsFromChTable(table: Table): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           sess
             .createStatement
             .executeQuery(
               s"delete from ${table.schema}.${table.name} where ${table.where_filter.getOrElse(" ")}"
             )
         }.tapError(er => ZIO.logError(er.getMessage))
           .refineToOrDie[SQLException]
           .when(
             table.recreate == 0 &&
               table.operation == AppendWhere
           )
  } yield ()

  private def dropChTable(fullTableName: String): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.logInfo(s"dropChTable fullTableName = $fullTableName")
    _ <- ZIO.attemptBlockingInterrupt {
           sess.createStatement.executeQuery(s"drop table if exists $fullTableName")
         }.refineToOrDie[SQLException]
  } yield ()

  private def createEmptyChTable(
    createChTableScript: Option[String]
  ): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.logInfo(s"createEmptyChTable script = $createChTableScript")
    _ <- ZIO.attemptBlockingInterrupt {
           createChTableScript match {
             case Some(query) => sess.createStatement.executeQuery(query)
             case None        => ()
           }
         }.refineToOrDie[SQLException]
  } yield ()

  private def recreate(
    table: Table,
    createChTableScript: Option[String]
  ): ZIO[Any, SQLException, Unit] = for {
    _ <- dropChTable(table.fullTableName()).when(table.recreate == 1)
    _ <- createEmptyChTable(createChTableScript).when(table.recreate == 1)
  } yield ()

  def recreateTableCopyData(
    table: Table,
    batch_size: Int,
    fetch_size: Int,
    maxValCnt: Option[MaxValAndCnt],
    createChTableScript: Option[String],
    appendKeys: Option[List[Any]]
  ): ZIO[Any, SQLException, Long] =
    for {
      start  <- Clock.currentTime(TimeUnit.MILLISECONDS)
      _      <-
        ZIO.logInfo(
          s"recreateTableCopyData CNT_ROWS before coping = ${maxValCnt.map(_.CntRows).getOrElse(0L)}"
        ) // todo: remove
      _      <- recreate(table, createChTableScript).when(table.recreate == 1)
      rows   <- ZIO.attemptBlockingInterrupt {
                  val whereFilter: String           = table.whereFilter(maxValCnt, appendKeys)
                  val insertDataBridgeQuery: String =
                    s"""
                     |insert into ${table.fullTableName()}
                     |select ${table.only_columns.getOrElse("*").toUpperCase()}
                     |from jdbc('ora?fetch_size=$fetch_size&batch_size=$batch_size',
                     |          'select ${table.only_columns.getOrElse("*").toUpperCase()}
                     |             from ${table.fullTableName()}
                     |             $whereFilter
                     |             ${table.orderBy()}'
                     |         )
                     |""".stripMargin
                  println(s"insertDataBridgeQuery = $insertDataBridgeQuery")
                  sess.createStatement.executeQuery(insertDataBridgeQuery)
                  val rsRowCnt                      = sess
                    .createStatement
                    .executeQuery(
                      s"select sum(1) as cnt from ${table.schema}.${table.name}"
                    ) // todo: use dict system.xxx
                  rsRowCnt.next()
                  val rowCount = rsRowCnt.getLong(1)
                  rsRowCnt.close()
                  println(s"recreateTableCopyData INSERTED debug rowCount = $rowCount")
                  rowCount - maxValCnt.map(_.CntRows).getOrElse(0L)
                }.refineToOrDie[SQLException]
      finish <- Clock.currentTime(TimeUnit.MILLISECONDS)
      _      <- ZIO.logInfo(
                  s"Copied $rows rows to ${table.schema}.${table.name} with ${finish - start} ms."
                )
    } yield rows

  def prepareDictUpdTable(
    dictName: String,
    createDictScript: String,
    tableName: String,
    createUpdTblScript: String
  ): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           sess.createStatement.executeQuery(s"drop dictionary if exists $dictName")
           sess.createStatement.executeQuery(s"drop table if exists $tableName")
           sess.createStatement.executeQuery(createUpdTblScript)
           sess.createStatement.executeQuery(createDictScript)
         }
           .refineToOrDie[SQLException]
  } yield ()

  private def populateUpdTable(
    updTableName: String,
    table: Table,
    fetch_size: Int,
    batch_size: Int
  ): ZIO[Any, SQLException, Long] = for {
    start   <- Clock.currentTime(TimeUnit.MILLISECONDS)
    rowsCnt <- ZIO.attemptBlockingInterrupt {
                 val whereFilter: String         =
                   table.where_filter.map(flt => s" where $flt").getOrElse(" ")
                 val scriptInsertIntoUpd: String =
                   s"""
                      |insert into $updTableName
                      |select ${table.only_columns.getOrElse("*").toUpperCase}
                      |from jdbc('ora?fetch_size=$fetch_size&batch_size=$batch_size',
                      |          'select ${table.only_columns.getOrElse("*").toUpperCase}
                      |             from ${table.fullTableName()}
                      |             $whereFilter
                      |             '
                      |         )
                      |""".stripMargin
                 println(s"populateUpdTable = $scriptInsertIntoUpd")
                 sess.createStatement.executeQuery(scriptInsertIntoUpd)
                 val rsRowCnt                    = sess
                   .createStatement
                   .executeQuery(s"select sum(1) as cnt from $updTableName")
                 rsRowCnt.next()
                 val rowCount                    = rsRowCnt.getLong(1)
                 rsRowCnt.close()
                 rowCount
               }.refineToOrDie[SQLException]
    finish  <- Clock.currentTime(TimeUnit.MILLISECONDS)
    _       <-
      ZIO.logInfo(s"populateUpdTable executed with ${finish - start} ms. inserted $rowsCnt rows.")
  } yield rowsCnt

  def prepareStructsForUpdate(
    updateStructsScripts: UpdateStructsScripts,
    table: Table,
    fetch_size: Int,
    batch_size: Int,
    @nowarn pkColList: List[String]
  ): ZIO[Any, SQLException, Long] =
    for {
      start       <- Clock.currentTime(TimeUnit.MILLISECONDS)
      updTableName = s"${table.schema}.upd_${table.name}"
      updDictName  = s"${table.schema}.dict_${table.name}"
      _           <- prepareDictUpdTable(
                       updDictName,
                       updateStructsScripts.dictScript,
                       updTableName,
                       updateStructsScripts.updTableScript
                     )
      cntRows     <- populateUpdTable(updTableName, table, fetch_size, batch_size)
    } yield cntRows

  def copyTableChOra(meta: ViewQueryMeta): ZIO[Any, SQLException, Unit] = for {
    start  <- Clock.currentTime(TimeUnit.MILLISECONDS)
    _      <- ZIO.attemptBlockingInterrupt {
                val copyJdbcScript: String =
                  s"""
                |insert into ${meta.oraSchema}.${meta.oraTable}(${meta.copyChOraColumns})
                |select ${meta.copyChOraColumns}
                |from   ${meta.chSchema}.${meta.chTable}
                |""".stripMargin
                sess.createStatement.executeQuery(copyJdbcScript)
              }.refineToOrDie[SQLException]
    finish <- Clock.currentTime(TimeUnit.MILLISECONDS)
    _      <- ZIO.logInfo(s"copyTableChOra (ch -> ora) executed with ${finish - start} ms.")
  } yield ()

  /*  //todo: delete it
  def recreateTmpTableForUpdate(
    table: Table,
    rs: ZIO[Any, SQLException, ResultSet],
    batch_size: Int,
    pkColList: List[String]
  ): ZIO[Any, SQLException, Long] =
    for {
      _               <- ZIO.unit
      updTableName     = s"upd_${table.name}"
      updDictName      = s"dict_${table.name}"
      _               <- ZIO.logInfo(s"temp table for update : $updTableName - $tableType")
      oraRs           <- rs
      _               <- debugRsColumns(oraRs)
      cols             = (1 to oraRs.getMetaData.getColumnCount)
                           .map(i =>
                             OraChColumn(
                               oraRs.getMetaData.getColumnName(i).toLowerCase,
                               oraRs.getMetaData.getColumnTypeName(i),
                               oraRs.getMetaData.getColumnClassName(i),
                               oraRs.getMetaData.getColumnDisplaySize(i),
                               oraRs.getMetaData.getPrecision(i),
                               oraRs.getMetaData.getScale(i),
                               oraRs.getMetaData.isNullable(i)//,
                               //table.notnull_columns
                             )
                           )
                           .toList
      nakedCols        = cols.map(chCol => chCol.name).mkString(",\n")
      colsScript       = cols.map(chCol => chCol.clColumnString).mkString(",\n")
      pkColumns        = pkColList.mkString(",")
      updTableFullName = s"${table.schema}.$updTableName"

      createScript = s"""create table $updTableFullName
                            |(
                            | $colsScript
                            |) ENGINE = MergeTree PRIMARY KEY ($pkColumns)
                            |""".stripMargin

      _           <- ZIO.logDebug(s"update temp table SCRIPT : $createScript")

      insertQuery =
        s"""insert into ${table.schema}.$updTableName
           |select $nakedCols
           |from
           |input('$colsScript
           |      ')
           |""".stripMargin

      // If MergeTree then drop dictionary if exists
      _          <- ZIO.attemptBlockingInterrupt {
                      sess
                        .createStatement
                        .executeQuery(s"drop dictionary if exists ${table.schema}.$updDictName")
                    }.when(tableType == MergeTree)
                      .refineToOrDie[SQLException]

      _ <- ZIO.attemptBlockingInterrupt {
             sess
               .createStatement
               .executeQuery(s"drop table if exists ${table.schema}.$updTableName")
             sess.createStatement.executeQuery(createScript)
             createScript
           }.refineToOrDie[SQLException]

      // If MergeTree then create dictionary
      _ <- ZIO.attemptBlockingInterrupt {
             val script =
               s"""
                  |create dictionary ${table.schema}.$updDictName
                  | (
                  | $colsScript
                  |)
                  |PRIMARY KEY ($pkColumns)
                  |SOURCE(CLICKHOUSE(
                  |					user     '$chAmdinPass'
                  |					password '$chAmdinPass'
                  |					db       '${table.schema}'
                  |					table    '$updTableName'
                  |      ))
                  |LAYOUT(COMPLEX_KEY_DIRECT)
                  |""".stripMargin
             sess
               .createStatement
               .executeQuery(script)
             script
           }.when(tableType == MergeTree)
             .tap(s =>
               ZIO.logInfo(s"create dictionary SCRIPT: ${s.map(_.replace(chAmdinPass, "***"))}")
             )
             .refineToOrDie[SQLException]

      rowsWithQuery <- ZIO.attemptBlockingInterrupt {
                         // ------------------------------------
                         val ps: PreparedStatement = sess.prepareStatement(insertQuery)
                         Iterator.continually(oraRs).takeWhile(_.next()).foldLeft(1) {
                           case (counter, rs) =>
                             cols.foldLeft(1) { case (i, c) =>
                               (c.typeName, c.scale) match {
                                 case ("NUMBER", 0)   =>
                                   val l = rs.getLong(c.name)
                                   if (rs.wasNull())
                                     ps.setNull(i, Types.BIGINT)
                                   else
                                     ps.setLong(i, l)
                                 case ("NUMBER", _)   =>
                                   val n = rs.getDouble(c.name)
                                   if (rs.wasNull())
                                     ps.setNull(i, Types.DOUBLE)
                                   else
                                     ps.setDouble(i, n)
                                 case ("CLOB", _)     => ps.setString(i, rs.getString(c.name))
                                 case ("VARCHAR2", _) =>
                                   val s: String = rs.getString(c.name)
                                   if (rs.wasNull())
                                     ps.setNull(i, Types.VARCHAR)
                                   else
                                     ps.setString(i, s)
                                 case ("DATE", _)     =>
                                   val tmp             = rs.getString(c.name)
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
                                     ps.setNull(i, Types.DATE)
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

                         val rsRowCount = sess
                           .createStatement
                           .executeQuery(
                             s""" select t.total_rows
                                |from system.tables t
                                |where t.database = '${table.schema}' and
                                |      t.name     = '$updTableName' """.stripMargin
                           )
                         rsRowCount.next()
                         val rowCount   = rsRowCount.getLong(1)
                         rsRowCount.close()
                         AppendRowsWQuery(rowCount, insertQuery)
                       }.tapBoth(
                         er => ZIO.logError(er.getMessage),
                         rq => ZIO.logDebug(s"recreateTableCopyDataForUpdate insert = ${rq.query}")
                       ).refineToOrDie[SQLException]
      _             <- ZIO.logInfo(s"Copied ${rowsWithQuery.copied} rows.")
    } yield rowsWithQuery.copied*/

  def createDatabases(schemas: Set[String]): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.unit
    _ <- ZIO.foreachDiscard(schemas) { schema =>
           ZIO.attemptBlockingInterrupt {
             val query: String =
               s"CREATE DATABASE IF NOT EXISTS $schema"
             val rs: ResultSet = sess.createStatement.executeQuery(query)
             rs.next()
             query
           }.tapBoth(
             er => ZIO.logError(er.getMessage),
             createDb => ZIO.logDebug(s"createDatabases script = $createDb")
           ).refineToOrDie[SQLException]
         }
  } yield ()

  def truncateTable(meta: ViewQueryMeta): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           val rs: ResultSet =
             sess.createStatement.executeQuery(s"truncate table ${meta.chSchema}.${meta.chTable}")
           rs.close()
         }.tapError(er => ZIO.logError(er.getMessage))
           .refineToOrDie[SQLException]
  } yield ()

  def getChTableResultSet(meta: ViewQueryMeta): ZIO[Any, SQLException, ResultSet] = for {
    rs <- ZIO.attemptBlockingInterrupt {
            val selectQuery: String = s"select * from ${meta.chSchema}.${meta.chTable}"
            println(s"getChTableResultSet selectQuery = $selectQuery")
            sess.createStatement.executeQuery(selectQuery)
          }.tapError(er => ZIO.logError(er.getMessage) // ,
          // select => ZIO.logDebug(s"getChTableResultSet")
          ).refineToOrDie[SQLException]
  } yield rs

  def insertFromQuery(
    meta: ViewQueryMeta,
    calcParams: Set[CalcParams]
  ): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           val mapCalcParams: Map[String, String] =
             calcParams.iterator.map(p => p.name -> p.value.trim).toMap
           val strQuery: String                   = meta.query.getOrElse(" ")
           val selectQuery: String                =
             meta.params.toList.sortBy(_.ord).foldLeft(strQuery) { case (r, c) =>
               c.chType match {
                 case "Decimal(38,6)" => r.replace(c.name, mapCalcParams.getOrElse(c.name, "*****"))
                 case "String"        =>
                   r.replace(c.name, s"'${mapCalcParams.getOrElse(c.name, "*****")}'")
                 case "UInt32"        => r.replace(c.name, mapCalcParams.getOrElse(c.name, "*****"))
               }
             }
           val insQuery: String                   = s"insert into ${meta.chSchema}.${meta.chTable} $selectQuery"
           val rs: ResultSet                      = sess.createStatement.executeQuery(insQuery)
           rs.close()
           insQuery
         }.tapError(er => ZIO.logError(er.getMessage)
         // query => ZIO.logDebug(s"insertFromQuery query = $query")
         ).refineToOrDie[SQLException]
  } yield ()

}

trait jdbcChSession {
  // def sess(taskId: Int): ZIO[Any, SQLException, chSess]
  val props = new Properties()
  def getClickHousePool(): ZIO[Any, SQLException, ClickHouseDataSource]
}

case class jdbcSessionImpl(ch: ClickhouseServer) extends jdbcChSession {

  /*  private val ds = {
  }*/

  override def getClickHousePool(): ZIO[Any, SQLException, ClickHouseDataSource] = for {
    start  <- Clock.currentTime(TimeUnit.MILLISECONDS)
    _      <-
      ZIO.logInfo(
        s"chConnection at this time internally new Clickhouse connection pool created............ "
      )
    sess   <- ZIO.attemptBlockingInterrupt {
                props.setProperty("http_connection_provider", "HTTP_URL_CONNECTION")
                // property in ms.
                props.setProperty("connection_timeout", "10000000")
                props.setProperty("socket_timeout", "10000000")
                props.setProperty("dataTransferTimeout", "10000000")
                props.setProperty("timeToLiveMillis", "10000000")
                props.setProperty("socket_keepalive", "true")
                props.setProperty("http_receive_timeout", "10000000")
                props.setProperty("keep_alive_timeout", "10000000")
                props.setProperty("user", ch.user);
                props.setProperty("password", ch.password);
                props.setProperty("client_name", "orach");
                val dataSource: ClickHouseDataSource = new ClickHouseDataSource(ch.getUrl(), props)
                // val conn: ClickHouseConnection = dataSource.getConnection(ch.user, ch.password)
                // chSess(conn, taskId)
                dataSource
              }.refineToOrDie[SQLException]
    finish <- Clock.currentTime(TimeUnit.MILLISECONDS)
    _      <-
      ZIO.logInfo(
        s"chConnection ${finish - start} ms."
      )
  } yield sess

  // todo: was moved to class constructor. Purpose: we need has one pool
  /*  override def chConnection(taskId: Int): ZIO[Any, SQLException, chSess] = for {
    start <- Clock.currentTime(TimeUnit.MILLISECONDS)
    _ <- ZIO.logInfo(s"chConnection at this time internally new Clickhouse connection pool created............ ")
    sess <- ZIO.attemptBlockingInterrupt {
      props.setProperty("http_connection_provider", "HTTP_URL_CONNECTION")
      // property in ms.
      props.setProperty("connection_timeout", "720000")
      props.setProperty("socket_timeout", "720000")
      props.setProperty("dataTransferTimeout", "720000")
      props.setProperty("timeToLiveMillis", "720000")
      props.setProperty("socket_keepalive", "true")
      val dataSource                 = new ClickHouseDataSource(ch.getUrl(), props)
      val conn: ClickHouseConnection = dataSource.getConnection(ch.user, ch.password)
      chSess(conn, taskId)
    }.refineToOrDie[SQLException]
    finish  <- Clock.currentTime(TimeUnit.MILLISECONDS)
    _              <-
      ZIO.logInfo(
        s"chConnection ${finish - start} ms."
      )
  } yield sess*/

  /*  def sess(taskId: Int): ZIO[Any, SQLException, chSess] = for {
    session <- chConnection(taskId)
  } yield session*/

}

object jdbcChSessionImpl {

  val layer: ZLayer[ClickhouseServer with ClickhouseServer, SQLException, jdbcChSession] =
    ZLayer {
      for {
        chs <- ZIO.service[ClickhouseServer]
      } yield jdbcSessionImpl(chs)
    }

}
