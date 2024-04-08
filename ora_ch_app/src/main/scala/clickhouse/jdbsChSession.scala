package clickhouse

import calc.{ CalcParams, ViewQueryMeta }
import column.OraChColumn
import com.clickhouse.client.config.ClickHouseClientOption
import com.clickhouse.client.http.config.HttpConnectionProvider
import com.clickhouse.jdbc.{ ClickHouseConnection, ClickHouseDataSource, ClickHouseDriver }
import common.{ AppendRowsWQuery, LeftJoin, MergeTree, UpdateTableType }
import conf.ClickhouseServer
import table._
import zio.{ Clock, ZIO, ZLayer }

import java.sql.{ Connection, DriverManager, PreparedStatement, ResultSet, SQLException, Types }
import java.time.{ LocalDateTime, ZoneOffset }
import java.time.format.DateTimeFormatter
import java.util.{ Calendar, Properties }
import common.Types._
import request.AppendWhere

import java.util.concurrent.TimeUnit

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

  def updateLeftJoin(
    updateTable: Table,
    updTable: Table,
    primaryKeyColumnsCh: List[String]
  ): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           val pkColsStr     = primaryKeyColumnsCh.mkString(",")
           val updateColsSql = updateTable
             .update_fields
             .getOrElse("empty_update_fields")
             .split(",")
             .map { col =>
               s"$col = joinGet(${updateTable.schema}.${updTable.name}, '$col', $pkColsStr)"
             }
             .mkString(",")
           val updateQuery   =
             s"""
                |ALTER TABLE ${updateTable.schema}.${updateTable.name}
                |UPDATE
                |       $updateColsSql
                |where 1>0;
                |""".stripMargin
           println(s"DEBUG updateLeftJoin = $updateQuery")
           val rs            = sess.createStatement.executeQuery(updateQuery)
           rs.close()
           updateQuery
         }.tapBoth(
           er => ZIO.logError(er.getMessage),
           update => ZIO.logDebug(s"updateColumns query = $update")
         ).refineToOrDie[SQLException]
  } yield ()

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

           println(s"DEBUG updateMergeTree = $updateQuery")

           val rs = sess.createStatement.executeQuery(updateQuery)
           rs.close()
           updateQuery
         }.tapBoth(
           er => ZIO.logError(er.getMessage),
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
  def getCountCopiedRows(table: Table): ZIO[Any, Nothing, Long] = for {
    rows <- ZIO.attemptBlockingInterrupt {
              val rsRowCount = sess
                .createStatement
                // todo: for performance reason take this info from system.tables
                // instead of from direct table.
                .executeQuery(s"select count() as cnt from ${table.schema}.${table.name}")
              rsRowCount.next()
              val rowCount   = rsRowCount.getLong(1)
              rowCount
            }.refineToOrDie[SQLException] orElse ZIO.succeed(0L)
  } yield rows

  def getCountCopiedRowsFUpd(table: Table): ZIO[Any, Nothing, Long] = for {
    rows <- ZIO.attempt {
              val rsRowCount = sess
                .createStatement
                .executeQuery(s""" select t.total_rows
                                 | from system.tables t
                                 | where t.database = '${table.schema}' and
                                 |      t.name      = '${table.name}' """.stripMargin)
              rsRowCount.next()
              val rowCount   = rsRowCount.getLong(1)
              rowCount
            }.refineToOrDie[SQLException] orElse ZIO.succeed(0L)
  } yield rows

  private def debugRsColumns(rs: ResultSet): ZIO[Any, Nothing, Unit] = for {
    _ <- ZIO.foreachDiscard(1 to rs.getMetaData.getColumnCount) { i =>
           ZIO.logTrace(s"""${rs.getMetaData.getColumnName(i).toLowerCase} -
                           |${rs.getMetaData.getColumnTypeName(i)} -
                           |${rs.getMetaData.getColumnClassName(i)} -
                           |${rs.getMetaData.getColumnDisplaySize(i)} -
                           |${rs.getMetaData.getPrecision(i)} -
                           |${rs.getMetaData.getScale(i)}
                           |""".stripMargin)
         }
  } yield ()

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

  def recreateTableCopyData(
    table: Table,
    rs: ZIO[Any, SQLException, ResultSet],
    batch_size: Int,
    maxValCnt: Option[MaxValAndCnt]
  ): ZIO[Any, SQLException, Long] =
    for {
      _         <- ZIO.logInfo(
                     s"recreateTableCopyData table : ${table.fullTableName()} batch_size = $batch_size"
                   )
      oraRs     <- rs
      _         <- debugRsColumns(oraRs)
      cols       = (1 to oraRs.getMetaData.getColumnCount)
                     .map(i =>
                       OraChColumn(
                         oraRs.getMetaData.getColumnName(i).toLowerCase,
                         oraRs.getMetaData.getColumnTypeName(i),
                         oraRs.getMetaData.getColumnClassName(i),
                         oraRs.getMetaData.getColumnDisplaySize(i),
                         oraRs.getMetaData.getPrecision(i),
                         oraRs.getMetaData.getScale(i),
                         oraRs.getMetaData.isNullable(i),
                         table.notnull_columns
                       )
                     )
                     .toList
      nakedCols  = cols.map(chCol => chCol.name).mkString(",\n")
      colsScript = cols.map(chCol => chCol.clColumnString).mkString(",\n")
      _         <- ZIO.logDebug(s"pk_columns = ${table.pk_columns}")

      createScript =
        s"""create table ${table.schema}.${table.name}
           |(
           | $colsScript
           |) ENGINE = MergeTree()
           | ${table.partition_by match {
            case Some(part_by) => s"PARTITION BY ($part_by)"
            case None          => " "
          }}
           | PRIMARY KEY (${table.pk_columns})
           |""".stripMargin
      _           <- ZIO.logDebug(s"createScript = $createScript")
      insQuer      =
        s"""insert into ${table.schema}.${table.name}
           |select $nakedCols
           |from
           |input('$colsScript
           |      ')
           |""".stripMargin
      _           <- ZIO.logDebug(s"insQuer = $insQuer")

      _              <- ZIO.attemptBlockingInterrupt {
                          sess
                            .createStatement
                            .executeQuery(s"drop table if exists ${table.schema}.${table.name}")
                          sess.createStatement.executeQuery(createScript)
                        }.tapError(er => ZIO.logError(er.getMessage))
                          .refineToOrDie[SQLException]
                          .when(table.recreate == 1)
      dtBeforeCoping <- Clock.currentTime(TimeUnit.MILLISECONDS)
      rows           <- ZIO.attemptBlockingInterrupt {
                          val ps: PreparedStatement = sess.prepareStatement(insQuer)
                          var beforeBuildBatch      = System.currentTimeMillis()
                          var sumBuildBatch         = 0L
                          var sumExecBatch          = 0L
                          Iterator.continually(oraRs).takeWhile(_.next()).foldLeft(1) { case (counter, rs) =>
                            cols.foldLeft(1) { case (i, c) =>
                              (c.typeName, c.scale) match {
                                case ("NUMBER", 0)   => ps.setLong(i, rs.getLong(c.name))
                                case ("NUMBER", _)   => ps.setDouble(i, rs.getDouble(c.name))
                                case ("CLOB", _)     => ps.setString(i, rs.getString(c.name))
                                case ("VARCHAR2", _) => ps.setString(i, rs.getString(c.name))
                                /*
                                // -------------------------------------
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
                                  else {
                                    ps.setString(i, s)
                       */
                                // ----------------------------------------------------------------
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
                              println(
                                s"buildBatch = ${System.currentTimeMillis() - beforeBuildBatch} ms."
                              )
                              sumBuildBatch = sumBuildBatch + System.currentTimeMillis() - beforeBuildBatch
                              beforeBuildBatch = System.currentTimeMillis()
                              val beforeExecBatch = System.currentTimeMillis()
                              ps.executeBatch()
                              println(
                                s"executeBatch = ${System.currentTimeMillis() - beforeExecBatch} ms.  counter = $counter"
                              )
                              sumExecBatch = sumExecBatch + System.currentTimeMillis() - beforeExecBatch
                              0
                            } else
                              counter + 1
                          }
                          ps.executeBatch()
                          val rsRowCount            = sess
                            .createStatement
                            .executeQuery(s"select sum(1) as cnt from ${table.schema}.${table.name}")
                          rsRowCount.next()
                          val rowCount              = rsRowCount.getLong(1)
                          rsRowCount.close()

                          println(s"sumBuildBatch = $sumBuildBatch ms.")
                          println(s"sumExecBatch = $sumExecBatch ms.")
                          rowCount - maxValCnt.map(_.CntRows).getOrElse(0L)
                        }.refineToOrDie[SQLException]
      dtAfterCoping  <- Clock.currentTime(TimeUnit.MILLISECONDS)
      _              <-
        ZIO.logInfo(
          s"Copied $rows rows to ${table.schema}.${table.name} with ${dtAfterCoping - dtBeforeCoping} ms."
        )
    } yield rows

  def recreateTmpTableForUpdate(
    table: Table,
    rs: ZIO[Any, SQLException, ResultSet],
    batch_size: Int,
    pkColList: List[String],
    tableType: UpdateTableType
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
                               oraRs.getMetaData.isNullable(i),
                               table.notnull_columns
                             )
                           )
                           .toList
      nakedCols        = cols.map(chCol => chCol.name).mkString(",\n")
      colsScript       = cols.map(chCol => chCol.clColumnString).mkString(",\n")
      pkColumns        = pkColList.mkString(",")
      updTableFullName = s"${table.schema}.$updTableName"

      createScript = tableType match {
                       case LeftJoin  =>
                         s"""create table $updTableFullName
                            |(
                            | $colsScript
                            |) ENGINE = Join(ANY, LEFT, $pkColumns) SETTINGS join_use_nulls = 1
                            |""".stripMargin
                       case MergeTree =>
                         s"""create table $updTableFullName
                            |(
                            | $colsScript
                            |) ENGINE = MergeTree PRIMARY KEY ($pkColumns)
                            |""".stripMargin
                     }
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
    } yield rowsWithQuery.copied

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
  def sess(taskId: Int): ZIO[Any, SQLException, chSess]
  val props = new Properties()
  def chConnection(taskId: Int): ZIO[Any, SQLException, chSess]
}

case class jdbcSessionImpl(ch: ClickhouseServer) extends jdbcChSession {

  def sess(taskId: Int): ZIO[Any, SQLException, chSess] = for {
    session <- chConnection(taskId)
    cnt      = props.keySet().size()
    _       <- ZIO.logDebug(s"Connection has $cnt properties")
    keys     = props.keySet().toArray.map(_.toString).toList
    _       <- ZIO.foreachDiscard(keys)(k => ZIO.logDebug(s"$k - ${props.getProperty(k)}]"))
  } yield session

  override def chConnection(taskId: Int): ZIO[Any, SQLException, chSess] = for {
    _    <- ZIO.unit
    sess <- ZIO.attemptBlockingInterrupt {
              props.setProperty("http_connection_provider", "HTTP_URL_CONNECTION")
              props.setProperty("socket_timeout", "120000")      // property in ms. 120 seconds.
              props.setProperty("dataTransferTimeout", "240000") // property in ms. 120 seconds.
              val dataSource                 = new ClickHouseDataSource(ch.getUrl, props)
              val conn: ClickHouseConnection = dataSource.getConnection(ch.user, ch.password)
              chSess(conn, taskId)
            }.refineToOrDie[SQLException]
  } yield sess

}

object jdbcChSessionImpl {

  val layer: ZLayer[ClickhouseServer, SQLException, jdbcChSession] =
    ZLayer {
      for {
        chs <- ZIO.service[ClickhouseServer]
      } yield jdbcSessionImpl(chs)
    }

}
