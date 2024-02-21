package clickhouse

import calc.{ CalcParams, ViewQueryMeta }
import column.OraChColumn
import com.clickhouse.client.config.ClickHouseClientOption
import com.clickhouse.client.http.config.HttpConnectionProvider
import com.clickhouse.jdbc.{ ClickHouseConnection, ClickHouseDataSource, ClickHouseDriver }
import common.AppendRowsWQuery
import conf.ClickhouseServer
import table._
import zio.{ ZIO, ZLayer }

import java.sql.{ Connection, DriverManager, PreparedStatement, ResultSet, SQLException, Types }
import java.time.{ LocalDateTime, ZoneOffset }
import java.time.format.DateTimeFormatter
import java.util.Properties
import common.Types._

case class chSess(sess: Connection, taskId: Int) {

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

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

  def updateColumns(
    updateTable: Table,
    updTable: Table,
    pkColumns: List[String]
  ): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           val pkColsStr     = pkColumns.mkString(",")
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
           val rs            = sess.createStatement.executeQuery(updateQuery)
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
    tblExistsCh <- isTableExistsCh(table)
                     .when(
                       table.sync_by_column_max.nonEmpty ||
                         table.sync_update_by_column_max.nonEmpty ||
                         table.sync_by_columns.nonEmpty
                     )
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
           }.tapError(er => ZIO.logError(er.getMessage))
             .refineToOrDie[SQLException]
  } yield res

  def whereAppendInt1(table: Table): ZIO[Any, SQLException, Option[List[Int]]] = for {
    res <- getSyncWhereFilterRsTuples(table)
  } yield Some(res.map(_.asInstanceOf[Int]))

  def whereAppendInt2(table: Table): ZIO[Any, SQLException, Option[List[(Int, Int)]]] = for {
    res <- getSyncWhereFilterRsTuples(table)
  } yield Some(res.map(_.asInstanceOf[(Int, Int)]))

  def whereAppendInt3(table: Table): ZIO[Any, SQLException, Option[List[(Int, Int, Int)]]] = for {
    res <- getSyncWhereFilterRsTuples(table)
  } yield Some(res.map(_.asInstanceOf[(Int, Int, Int)]))

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

  def recreateTableCopyData(
    table: Table,
    rs: ZIO[Any, SQLException, ResultSet],
    batch_size: Int,
    maxValCnt: Option[MaxValAndCnt]
  ): ZIO[Any, SQLException, Long] =
    for {
      _         <- ZIO.logInfo(s"recreateTableCopyData table : ${table.fullTableName()}")
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

      _ <- ZIO.attemptBlockingInterrupt {
             sess
               .createStatement
               .executeQuery(s"drop table if exists ${table.schema}.${table.name}")
             sess.createStatement.executeQuery(createScript)
           }.tapError(er => ZIO.logError(er.getMessage))
             .refineToOrDie[SQLException]
             .when(table.recreate == 1)

      rows <- ZIO.attemptBlockingInterrupt {
                val ps: PreparedStatement = sess.prepareStatement(insQuer)
                Iterator.continually(oraRs).takeWhile(_.next()).foldLeft(1) { case (counter, rs) =>
                  cols.foldLeft(1) { case (i, c) =>
                    (c.typeName, c.scale) match {
                      // Long - because getInt is that 4294967298 is outside the range of Java's int
                      case ("NUMBER", 0)   => ps.setLong(i, rs.getLong(c.name))
                      case ("NUMBER", _)   => ps.setDouble(i, rs.getDouble(c.name))
                      case ("CLOB", _)     => ps.setString(i, rs.getString(c.name))
                      case ("VARCHAR2", _) => ps.setString(i, rs.getString(c.name))
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
                val rsRowCount            = sess
                  .createStatement
                  .executeQuery(s"select sum(1) as cnt from ${table.schema}.${table.name}")
                rsRowCount.next()
                val rowCount              = rsRowCount.getLong(1)
                rsRowCount.close()
                rowCount - maxValCnt.map(_.CntRows).getOrElse(0L)
              }.refineToOrDie[SQLException]
      _    <- ZIO.logInfo(s"Copied $rows rows to ${table.schema}.${table.name}")
    } yield rows

  def recreateTableCopyDataForUpdate(
    table: Table,
    rs: ZIO[Any, SQLException, ResultSet],
    batch_size: Int,
    pkColList: List[String]
  ): ZIO[Any, SQLException, Long] =
    for {
      _           <- ZIO.logInfo(s"recreateTableCopyDataForUpdate table : ${table.fullTableName()}")
      oraRs       <- rs
      _           <- debugRsColumns(oraRs)
      cols         = (1 to oraRs.getMetaData.getColumnCount)
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
      nakedCols    = cols.map(chCol => chCol.name).mkString(",\n")
      colsScript   = cols.map(chCol => chCol.clColumnString).mkString(",\n")
      createScript =
        s"""create table ${table.schema}.${table.name}
           |(
           | $colsScript
           |) ENGINE = Join(ANY, LEFT, ${pkColList.mkString(",")}) SETTINGS join_use_nulls = 1
           |""".stripMargin
      _           <- ZIO.logDebug(s"createScript = $createScript")
      insertQuery  =
        s"""insert into ${table.schema}.${table.name}
           |select $nakedCols
           |from
           |input('$colsScript
           |      ')
           |""".stripMargin

      _ <- ZIO.attemptBlockingInterrupt {
             sess
               .createStatement
               .executeQuery(s"drop table if exists ${table.schema}.${table.name}")
             sess.createStatement.executeQuery(createScript)
             createScript
           }.tapBoth(
             er => ZIO.logError(er.getMessage),
             createScript =>
               ZIO.logDebug(s"recreateTableCopyDataForUpdate createScript = $createScript")
           ).refineToOrDie[SQLException]

      rowsWithQuery <- ZIO.attemptBlockingInterrupt {
                         // ------------------------------------
                         val ps: PreparedStatement = sess.prepareStatement(insertQuery)
                         Iterator.continually(oraRs).takeWhile(_.next()).foldLeft(1) {
                           case (counter, rs) =>
                             cols.foldLeft(1) { case (i, c) =>
                               (c.typeName, c.scale) match {
                                 // Long - because getInt is that 4294967298 is outside the range of Java's int
                                 case ("NUMBER", 0)   => ps.setLong(i, rs.getLong(c.name))
                                 case ("NUMBER", _)   => ps.setDouble(i, rs.getDouble(c.name))
                                 case ("CLOB", _)     => ps.setString(i, rs.getString(c.name))
                                 case ("VARCHAR2", _) => ps.setString(i, rs.getString(c.name))
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
                                |      t.name     = '${table.name}' """.stripMargin
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
          }.tapBoth(
            er => ZIO.logError(er.getMessage),
            select => ZIO.logDebug(s"getChTableResultSet query = $select")
          ).refineToOrDie[SQLException]
  } yield rs

  def insertFromQuery(
    meta: ViewQueryMeta,
    calcParams: Set[CalcParams]
  ): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           val mapCalcParams: Map[String, String] =
             calcParams.iterator.map(p => p.name -> p.value).toMap
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
         }.tapBoth(
           er => ZIO.logError(er.getMessage),
           query => ZIO.logDebug(s"insertFromQuery query = $query")
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
