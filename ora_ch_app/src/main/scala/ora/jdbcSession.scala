package ora

import calc.{ Query, VQParams, ViewQueryMeta }
import column.OraChColumn
import zio.{ Task, ZIO, _ }
import conf.OraServer
import oracle.jdbc.OracleDriver
import request.{ AppendByMax, AppendNotIn, AppendWhere, Recreate, SrcTable }
import table.Table

import java.sql.{
  Clob,
  Connection,
  DriverManager,
  PreparedStatement,
  ResultSet,
  SQLException,
  Statement
}
import java.util.Properties
import common.Types._
import OraChColumn._
import common.{ ResultSetWithQuery, SessCalc, SessTask, SessTypeEnum }
import connrepo.OraConnRepoImpl

sealed trait oraSess {
  def sess: Connection
  def getPid: Int = {
    val stmt: Statement     = sess.createStatement
    val rs: ResultSet       = stmt.executeQuery("select sys_context('USERENV','SID') as sid from dual")
    rs.next()
    val pg_backend_pid: Int = rs.getInt("sid")
    rs.close()
    pg_backend_pid
  }
}

case class oraSessCalc(sess: Connection, calcId: Int) extends oraSess {

  def getOraSessionId(): ZIO[Any, SQLException, Int] = for {
    sid <- ZIO.attemptBlockingInterrupt {
             val stmt: Statement = sess.createStatement
             val rs: ResultSet   =
               stmt.executeQuery("select sys_context('USERENV','SID') as sid from dual")
             rs.next()
             val sessionId: Int  = rs.getInt("sid")
             rs.close()
             sess.close()
             sessionId
           }.refineToOrDie[SQLException]
  } yield sid

  /**/
  def insertViewQueryLog(q: Query, id_reload_calc: Int): ZIO[Any, SQLException, Int] = for {
    _  <- ZIO.logInfo(s" insertViewQueryLog for idVq = ${q.query_id}")
    id <- ZIO.attemptBlockingInterrupt {
            val query: String        =
              s""" insert into ora_to_ch_query_log(id,id_vq,ora_sid,begin_calc,state,
                 | curr_date_context,analyt_datecalc,id_reload_calc)
                 | values(s_ora_to_ch_views_query_log.nextval, ${q.query_id}, sys_context('USERENV','SID'),
                 | sysdate, 'calculation',
                 | '${q.paramByName("curr_date_context")}',
                 | '${q.paramByName("analyt_datecalc")}',
                 | $id_reload_calc )
                 | """.stripMargin
            val idCol: Array[String] = Array("ID")
            val insertVqLog          = sess.prepareStatement(query, idCol)
            val affectedRows: Int    = insertVqLog.executeUpdate()
            val generatedKeys        = insertVqLog.getGeneratedKeys
            generatedKeys.next()
            val id: Int              = generatedKeys.getInt(1)
            sess.commit()
            sess.setClientInfo("OCSID.MODULE", "ORATOCH")
            sess.setClientInfo("OCSID.ACTION", s"calc_$id")
            sess.commit()
            insertVqLog.close()
            // sess.close()//todo: remove todo 0
            id
          }.tapError(er => ZIO.logError(er.getMessage))
            .refineToOrDie[SQLException]
    _  <- ZIO.logInfo(s"AFTER insertViewQueryLog id=$id")
  } yield id

  def saveEndCalculation(id: Int): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.logInfo(s"ORA saveEndCalculation for calcId = $calcId id=$id")
    _ <- ZIO.attemptBlockingInterrupt {
           val query: String =
             s""" update ora_to_ch_query_log l
                |   set l.end_calc = sysdate,
                |       l.state    = 'copying'
                | where l.id = $id
                | """.stripMargin
           val rs: ResultSet = sess.createStatement.executeQuery(query)
           rs.next()
           sess.commit()
           rs.close()
           // sess.close()
         }.tapError(er => ZIO.logError(er.getMessage))
           .refineToOrDie[SQLException]
  } yield ()

  def saveCalcError(id: Int, errorMsg: String): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           println(s"saveCalcError - id = $id")
           val query: String =
             s""" update ora_to_ch_query_log l
                |   set l.end_calc  = sysdate,
                |       l.error_msg = '$errorMsg',
                |       l.state     = 'error'
                | where l.id = $id
                | """.stripMargin
           val rs: ResultSet = sess.createStatement.executeQuery(query)
           sess.commit()
           rs.close()
         }.tapError(er => ZIO.logError(er.getMessage))
           .refineToOrDie[SQLException]
  } yield ()

  def saveBeginCopying(id: Int): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           val query: String =
             s""" update ora_to_ch_query_log l
                |   set l.begin_copy = sysdate,
                |       l.state    = 'copying'
                | where l.id = $id
                | """.stripMargin
           val rs: ResultSet = sess.createStatement.executeQuery(query)
           sess.commit()
           rs.close()
           // sess.close()//todo: remove todo #2
         }.tapError(er => ZIO.logError(er.getMessage))
           .refineToOrDie[SQLException]
  } yield ()

  def truncateTable(oraSchema: String, oraTable: String): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           val query: String =
             s" truncate table $oraSchema.$oraTable"
           val rs: ResultSet = sess.createStatement.executeQuery(query)
           sess.commit()
           rs.close()
           // sess.close() //todo: remove todo #3
         }.tapError(er => ZIO.logError(er.getMessage))
           .refineToOrDie[SQLException]
  } yield ()

  def saveEndCopying(id: Int): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           val query: String =
             s""" update ora_to_ch_query_log l
                |   set l.end_copy = sysdate,
                |       l.state    = 'finished',
                |       l.ora_ch_calc_dates_id =
                |       (select cd.id
                |          from ora_ch_calc_dates cd
                |         where cd.id_reload_calc    = l.id_reload_calc and
                |               cd.id_query          = l.id_vq and
                |               cd.analyt_datecalc   = to_number(replace(l.analyt_datecalc,'-',''))  and
                |               cd.curr_date_context = to_number(replace(l.curr_date_context,'-','')))
                | where l.id = $id
                | """.stripMargin
           val rs: ResultSet = sess.createStatement.executeQuery(query)
           sess.commit()
           rs.close()
           sess.close()
         }.tapError(er => ZIO.logError(er.getMessage))
           .refineToOrDie[SQLException]
  } yield ()

  def getQueryMeta(queryId: Int): ZIO[Any, SQLException, ViewQueryMeta] = for {
    queryMeta <- ZIO.attemptBlockingInterrupt {
                   val queryVq                 =
                     s""" select vq.ch_table,vq.ora_table,vq.query_text,vq.ch_schema,vq.ora_schema
                        |  from ora_to_ch_query vq
                        | where vq.id = $queryId """.stripMargin
                   val rsVq: ResultSet         = sess.createStatement.executeQuery(queryVq)
                   rsVq.next()
                   val queryParams             =
                     s""" select p.param_name,p.param_type,p.param_order
                        |  from ora_to_ch_query_params p
                        | where p.id_vq = $queryId """.stripMargin
                   val rsParams: ResultSet     = sess.createStatement.executeQuery(queryParams)
                   val vqParams: Set[VQParams] = Iterator
                     .continually(rsParams)
                     .takeWhile(_.next())
                     .map { rs =>
                       VQParams(
                         rs.getString("param_name"),
                         rs.getString("param_type"),
                         rs.getInt("param_order")
                       )
                     }
                     .toSet

                   val queryClob: Clob             = rsVq.getClob("query_text")
                   val isNull: Boolean             = rsVq.wasNull()
                   val optQueryStr: Option[String] =
                     if (isNull)
                       None
                     else
                       Some(queryClob.getSubString(1, queryClob.length().toInt))

                   val vqm = ViewQueryMeta(
                     rsVq.getString("ch_table"),
                     rsVq.getString("ora_table"),
                     optQueryStr,
                     vqParams,
                     rsVq.getString("ch_schema"),
                     rsVq.getString("ora_schema")
                   )
                   rsVq.close()
                   rsParams.close()

                   // sess.close()
                   vqm
                 }.tapError(er => ZIO.logError(er.getMessage))
                   .refineToOrDie[SQLException]
  } yield queryMeta

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

  def getColumnsFromRs(rs: ResultSet): ZIO[Any, SQLException, List[OraChColumn]] = for {
    _    <- debugRsColumns(rs)
    cols <- ZIO.attemptBlockingInterrupt {
              (1 to rs.getMetaData.getColumnCount)
                .map(i =>
                  OraChColumn(
                    rs.getMetaData.getColumnName(i).toLowerCase,
                    rs.getMetaData.getColumnTypeName(i),
                    rs.getMetaData.getColumnClassName(i),
                    rs.getMetaData.getColumnDisplaySize(i),
                    rs.getMetaData.getPrecision(i),
                    rs.getMetaData.getScale(i),
                    rs.getMetaData.isNullable(i),
                    " "
                  )
                )
                .toList
            }.tapError(er => ZIO.logError(er.getMessage))
              .refineToOrDie[SQLException]
  } yield cols

  def insertRsDataInTable(
    rs: ResultSet,
    tableName: String,
    oraSchema: String
  ): ZIO[Any, SQLException, Unit] = for {
    cols <- getColumnsFromRs(rs).tapError(er => ZIO.logError(er.getMessage))
    _    <- ZIO.attemptBlockingInterrupt {
              val batch_size       = 1000
              val fetch_size       = 1000
              val nakedCols        = cols.map(chCol => chCol.name).mkString(",")
              val bindQuests       = cols.map(_ => "?").mkString(",")
              val insQuery: String =
                s"insert into $oraSchema.$tableName($nakedCols) values($bindQuests)"
              // println(s" insQuery = $insQuery")

              val ps: PreparedStatement = sess.prepareStatement(insQuery)
              rs.setFetchSize(fetch_size)

              Iterator.continually(rs).takeWhile(_.next()).foldLeft(1) { case (counter, rs) =>
                cols.foldLeft(1) { case (i, c) =>
                  (c.typeName, c.typeClass, c.precision) match {
                    case (
                          "UInt8" | "UInt16" | "Int32" |
                          "Nullable(Int32)" | "UInt32" | "Int64" | "UInt64",
                          _,
                          _
                        ) =>
                      ps.setLong(i, rs.getLong(c.name))
                    case (_, "java.math.BigDecimal", _) => ps.setDouble(i, rs.getDouble(c.name))
                    case (_, "java.lang.String", _)     => ps.setString(i, rs.getString(c.name))
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
              sess.commit()
              rs.close()
              // sess.close()
            }.tapError(er => ZIO.logError(er.getMessage))
              .refineToOrDie[SQLException]
  } yield ()

}

case class oraSessTask(sess: Connection, taskId: Int) extends oraSess {

  private def setContext(table: Table): Unit =
    if (table.curr_date_context.nonEmpty && table.analyt_datecalc.nonEmpty) {
      val currDateContext = table.curr_date_context.getOrElse(" ")
      val analytDateCalc  = table.analyt_datecalc.getOrElse(" ")
      println(s" >>>>>>>>>>>>>> currDateContext = $currDateContext")
      println(s" >>>>>>>>>>>>>> analytDateCalc  = $analytDateCalc")
      val contextSql      =
        s"""
           | begin
           |   msk_analytics.set_curr_date_context('$currDateContext');
           |   DBMS_SESSION.SET_CONTEXT('CLIENTCONTEXT','ANALYT_DATECALC','$analytDateCalc');
           |end;
           |""".stripMargin
      val prep            = sess.prepareCall(contextSql)
      prep.execute()
    } else ()

  private def getAppendByFieldsPart(
    table: Table,
    appendKeys: Option[List[Any]] = Option.empty[List[Any]]
  ): String = {
    val appendKeysList: List[Any] = appendKeys.getOrElse(List.empty[Any])
    val filterStr                 = s" ( ${table.sync_by_columns.getOrElse(" EMPTY_SYNC_BY_COLUMNS ")} ) "
    val filterTuples: String      =
      if (table.syncArity() == 1) {
        s"(${appendKeysList.mkString(",")})"
      } else if (table.syncArity() == 2) {
        s"""(${appendKeysList
            .map(lst => s"(${lst.asInstanceOf[(Int, Int)].productIterator.toList.mkString(",")})")
            .mkString(",")})"""
      } else if (table.syncArity() == 3) {
        s"""(${appendKeysList
            .map(lst =>
              s"(${lst.asInstanceOf[(Int, Int, Int)].productIterator.toList.mkString(",")})"
            )
            .mkString(",")})"""
      } else
        " "
    if (appendKeys.nonEmpty)
      s" $filterStr not in $filterTuples "
    else
      " "
  }

  def getDataResultSet(
    taskId: Int,
    table: Table,
    fetch_size: Int,
    maxColCh: Option[MaxValAndCnt],
    appendKeys: Option[List[Any]] = Option.empty[List[Any]]
  ): ZIO[Any, SQLException, ResultSet] = for {
    sid <- ZIO.attemptBlockingInterrupt {
             getPid
           }.tapError(er => ZIO.logError(er.getMessage))
             .refineToOrDie[SQLException]
    _   <-
      ZIO.attemptBlockingInterrupt {
        val query: String =
          s""" update ora_to_ch_tasks_tables t
             |   set  ora_sid      = $sid
             | where t.id_task     = $taskId and
             |       t.schema_name = '${table.schema}' and
             |       t.operation   = '${table.operation.operStr}' and
             |       t.table_name  = '${table.name}' """.stripMargin
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        sess.setClientInfo("OCSID.MODULE", "ORATOCH")
        sess.setClientInfo("OCSID.ACTION", s"SLAVE_$sid")
        sess.commit()
        rs.close()
      }.tapError(er => ZIO.logError(er.getMessage))
        .refineToOrDie[SQLException]

    /*    _           <-
      ZIO.logDebug(
        s"getDataResultSet sid=$sid arity = ${table.syncArity()} appendKeys.nonEmpty = ${appendKeys.nonEmpty}"
      )*/
    _   <-
      ZIO.logInfo(s"getDataResultSet maxColCh.CntRows = ${maxColCh.map(_.CntRows).getOrElse(0L)}")

    rsWithQuery <- ZIO.attemptBlockingInterrupt {
                     val whereByFields: String = getAppendByFieldsPart(table, appendKeys)
                     val dataQuery             =
                       s""" select /*+ ALL_ROWS */ ${table.only_columns.getOrElse("*")} from ${table
                           .fullTableName()} ${table.operation match {
                           case Recreate    =>
                             s"${table.where_filter.map(where_filter => s" where $where_filter").getOrElse(" ")}"
                           case AppendWhere =>
                             table.where_filter match {
                               case Some(wf) => s" where $wf"
                               case None     => " "
                             }
                           case AppendByMax =>
                             (table.where_filter, maxColCh.map(_.MaxValue)) match {
                               case (Some(wf), Some(maxId)) =>
                                 s" where $wf and ${table.sync_by_column_max.getOrElse("X")} > $maxId "
                               case (Some(wf), None)        => s" where $wf "
                               case (None, Some(maxId))     =>
                                 s" where ${table.sync_by_column_max.getOrElse("X")} > $maxId "
                               case (None, None)            => " "
                             }
                           // in this case we don't add where_filter, you can use AppendWhere.
                           case AppendNotIn => s" where $whereByFields "
                           case _           => " "
                         }}
          ${table.ins_select_order_by.map(order => s" order by $order ").getOrElse(" ")}
        """.stripMargin

                     println(s"DEBUG_QUERY = $dataQuery")

                     setContext(table)
                     val dataRs = sess
                       .createStatement(
                         java.sql.ResultSet.TYPE_FORWARD_ONLY,
                         ResultSet.CONCUR_READ_ONLY
                       )
                       .executeQuery(dataQuery)
                     dataRs.setFetchSize(fetch_size)
                     ResultSetWithQuery(dataRs, dataQuery)
                   }.tapBoth(
                     er =>
                       ZIO.logError(
                         s"Exception: ${er.getMessage} curr_date_context=${table.curr_date_context}"
                       ),
                     rsq =>
                       ZIO.logDebug(s" >>>>>>>>>>>>>   getDataResultSet select = ${rsq.query} ")
                   ).refineToOrDie[SQLException]
    _           <- ZIO.logInfo(s"ORACLE getDataResultSet, QUERY = ${rsWithQuery.query}")
  } yield rsWithQuery.rs

  /**
   * Get oracle dataset for update clickhouse table. Select part contains only primary key columns
   * plus update_fields. In this method we only use next Json keys: name - table for update fields
   * in clickhouse. update_fields - fields that will updated in clickhouse table from oracle data.
   * where_filter - filter when select from oracle table. ins_select_order_by
   */
  def getDataResultSetForUpdate(
    table: Table,
    fetch_size: Int,
    pkColumns: List[String]
  ): ZIO[Any, SQLException, ResultSet] = for {
    _            <- ZIO.unit
    selectColumns = s"${pkColumns.mkString(",")},${table.updateColumns()} "
    rs           <- ZIO.attemptBlockingInterrupt {
                      val dataQuery                =
                        s"select /*+ ALL_ROWS */ $selectColumns from ${table.fullTableName()} ${table.whereFilter()}"
                      println(s"getDataResultSetForUpdate dataQuery = $dataQuery")
                      setContext(table)
                      val dateQueryWithOrd: String = s"$dataQuery ${table.orderBy()}"
                      val dataRs                   = sess
                        .createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
                        .executeQuery(dateQueryWithOrd)
                      dataRs.setFetchSize(fetch_size)
                      dataRs
                    }.tapError(er => ZIO.logError(er.getMessage))
                      .refineToOrDie[SQLException]
  } yield rs

  def saveTableList(tables: List[Table]): ZIO[Any, SQLException, Unit] = for {
    taskId           <- getTaskIdFromSess
    saveTablesEffects = tables.map { t =>
                          ZIO.attemptBlockingInterrupt {
                            val query: String =
                              s"insert into ora_to_ch_tasks_tables(id,id_task,schema_name,table_name,operation) " +
                                s" values(s_ora_to_ch_tasks_tables.nextval, $taskId,'${t.schema}','${t.name}','${t.operation.operStr}') "
                            val rs: ResultSet = sess.createStatement.executeQuery(query)
                            sess.commit()
                            rs.close()
                          }.tapError(er => ZIO.logError(er.getMessage))
                        }
    _                <- ZIO
                          .collectAll(saveTablesEffects)
                          .refineToOrDie[SQLException]
  } yield ()

  def setTaskState(state: String): ZIO[Any, SQLException, Unit] = for {
    taskId <- getTaskIdFromSess
    _      <-
      ZIO.attemptBlockingInterrupt {
        val query: String =
          s"update ora_to_ch_tasks set state='$state' where id=$taskId"
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        sess.commit()
        rs.close()
      }.tapError(er => ZIO.logError(er.getMessage))
        .refineToOrDie[SQLException]
  } yield ()

  def setTaskError(error: String, table: Table): ZIO[Any, SQLException, Unit] = for {
    taskId <- getTaskIdFromSess
    errMsg  = s"${table.fullTableName()} $error"
    _      <-
      ZIO.attemptBlockingInterrupt {
        val rsTask: ResultSet  = sess
          .createStatement
          .executeQuery(
            s"update ora_to_ch_tasks set state='error',end_datetime=sysdate, error_msg='$errMsg' where id=$taskId "
          )
        sess.commit()
        rsTask.close()
        val rsTable: ResultSet = sess
          .createStatement
          .executeQuery(
            s"""update ora_to_ch_tasks_tables t
               |set state = 'error',
               |    end_datetime = sysdate
               | where t.id_task     = $taskId and
               |       t.schema_name = '${table.schema}' and
               |       t.operation   = '${table.operation.operStr}' and
               |       t.table_name  = '${table.name}'
          """.stripMargin
          )
        sess.commit()
        rsTable.close()
      }.tapError(er => ZIO.logError(er.getMessage))
        .refineToOrDie[SQLException]
  } yield ()

  def setTableBeginCopy(table: Table): ZIO[Any, SQLException, Unit] = for {
    taskId <- getTaskIdFromSess
    sid    <- ZIO.attemptBlockingInterrupt {
                getPid
              }.tapError(er => ZIO.logError(er.getMessage))
                .refineToOrDie[SQLException]
    _      <-
      ZIO.attemptBlockingInterrupt {
        val query: String =
          s""" update ora_to_ch_tasks_tables t
             |   set  begin_datetime = sysdate,
             |        state          = 'copying',
             |        ora_sid        = $sid
             | where t.id_task     = $taskId and
             |       t.schema_name = '${table.schema}' and
             |       t.operation   = '${table.operation.operStr}' and
             |       t.table_name  = '${table.name}' """.stripMargin
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        sess.setClientInfo("OCSID.MODULE", "ORATOCH")
        sess.setClientInfo("OCSID.ACTION", s"SLAVE_$sid")
        sess.commit()
        rs.close()
      }.tapError(er => ZIO.logError(er.getMessage))
        .refineToOrDie[SQLException]
  } yield ()

  def updateCountCopiedRows(table: Table, rowCount: Long): ZIO[Any, Nothing, Long] = for {
    taskId <- getTaskIdFromSess
    // _ <- ZIO.logInfo(s"updateCountCopiedRows ${table.fullTableName()} taskId=$taskId rowCount=$rowCount")
    rows   <-
      ZIO.attemptBlockingInterrupt {
        val query: String =
          s""" update ora_to_ch_tasks_tables t
             |   set copied_records_count = $rowCount,
             |             speed_rows_sec = (case
             |                                when ((sysdate - t.begin_datetime)*24*60*60) != 0
             |                                then round($rowCount/((sysdate - t.begin_datetime)*24*60*60))
             |                                else $rowCount
             |                               end)
             | where t.id_task     = $taskId and
             |       t.schema_name = '${table.schema}' and
             |       t.operation   = '${table.operation.operStr}' and
             |       t.table_name  = '${table.name}' """.stripMargin
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        sess.commit()
        rs.close()
        rowCount
      }.tapError(er => ZIO.logError(er.getMessage))
        .tapDefect(df => ZIO.logError(df.toString)) orElse ZIO.succeed(0L)
  } yield rows

  def setTableCopied(table: Table, rowCount: Long): ZIO[Any, SQLException, Unit] =
    for {
      taskId <- getTaskIdFromSess
      _      <-
        ZIO.attemptBlockingInterrupt {
          val query: String =
            s""" update ora_to_ch_tasks_tables t
               |   set end_datetime = sysdate,
               |             state  = '${table.finishStatus()}',
               |             copied_records_count = $rowCount,
               |             speed_rows_sec = (case
               |                                when ((sysdate - t.begin_datetime)*24*60*60) != 0
               |                                then round($rowCount/((sysdate - t.begin_datetime)*24*60*60))
               |                                else $rowCount
               |                               end)
               | where t.id_task     = $taskId and
               |       t.schema_name = '${table.schema}' and
               |       t.operation   = '${table.operation.operStr}' and
               |       t.table_name  = '${table.name}' """.stripMargin
          val rs: ResultSet = sess.createStatement.executeQuery(query)
          sess.commit()
          rs.close()
        }.refineToOrDie[SQLException]
    } yield ()

  def taskFinished: ZIO[Any, SQLException, Unit] = for {
    taskId <- getTaskIdFromSess
    _      <- ZIO.attemptBlockingInterrupt {
                val query: String =
                  s"update ora_to_ch_tasks set end_datetime = sysdate, state='finished' where id = $taskId"
                val rs: ResultSet = sess.createStatement.executeQuery(query)
                sess.commit()
                rs.close()
              }.tapError(er => ZIO.logError(s"Error in taskFinished - ${er.getMessage}"))
                .refineToOrDie[SQLException]
  } yield ()

  def getTables(stables: List[SrcTable]): ZIO[Any, Nothing, List[Table]] =
    ZIO.collectAll {
      stables.flatMap { st =>
        st.tables.map { t =>
          ZIO.succeed(
            Table(
              st.schema,
              t.operation.getRecreate,
              t.operation,
              t.name,
              t.curr_date_context,
              t.analyt_datecalc,
              t.pk_columns,
              t.only_columns,
              t.ins_select_order_by,
              t.partition_by,
              t.notnull_columns,
              t.where_filter,
              t.sync_by_column_max,
              t.update_fields,
              t.sync_by_columns,
              t.sync_update_by_column_max
            )
          )
        }
      }
    }

  def getTaskIdFromSess: ZIO[Any, Nothing, Int] = ZIO.succeed(taskId)

}

trait jdbcSession {
  def sessTask(taskIdOpt: Option[Int] = Option.empty): ZIO[Any, SQLException, oraSessTask]
  def sessCalc(vqId: Int = 0, debugMsg: String): ZIO[Any, SQLException, oraSessCalc]
  val props = new Properties()
}

case class jdbcSessionImpl(oraRef: OraConnRepoImpl, sessType: SessTypeEnum) extends jdbcSession {

  println(s" jdbcSessionImpl main constructor sessType = $sessType")

  def sessTask(taskIdOpt: Option[Int] = Option.empty): ZIO[Any, SQLException, oraSessTask] = for {
    _       <- ZIO.logInfo(
                 s"[sessTask] jdbcSessionImpl.sess [${taskIdOpt.getOrElse(0)}] call oraConnectionTask"
               )
    session <- if (taskIdOpt.getOrElse(0) == 0)
                 oraConnectionTask()
               else
                 oraConnectionTaskEx(taskIdOpt)
  } yield session

  def sessCalc(vqId: Int, debugMsg: String): ZIO[Any, SQLException, oraSessCalc] = for {
    _       <- ZIO.logInfo(s"[sessCalc] jdbcSessionImpl.sess [$debugMsg] call oraConnectionCalc")
    session <- oraConnectionCalc(vqId)
  } yield session

  /**
   * When task executing now and taskId is known.
   */
  private def oraConnectionTaskEx(taskIdOpt: Option[Int]): ZIO[Any, SQLException, oraSessTask] =
    for {
      oraConn <- oraRef.getConnection().refineToOrDie[SQLException]
      sess    <- ZIO.attemptBlockingInterrupt {
                   oraConn.setAutoCommit(false)
                   oraConn.setClientInfo("OCSID.MODULE", "ORATOCH")
                   oraConn.setClientInfo("OCSID.ACTION", "SLAVE_INIT")
                   oraSessTask(oraConn, taskIdOpt.getOrElse(0))
                 }.tapError(er => ZIO.logError(er.getMessage))
                   .refineToOrDie[SQLException]
    } yield sess

  /**
   * When new task created.
   */
  private def oraConnectionTask(): ZIO[Any, SQLException, oraSessTask] = for {
    oraConn   <- oraRef.getConnection().refineToOrDie[SQLException]
    sessEffect = ZIO.attemptBlockingInterrupt {
                   val conn                 = oraConn
                   conn.setAutoCommit(false)
                   conn.setClientInfo("OCSID.MODULE", "ORATOCH")
                   val query: String        =
                     "insert into ora_to_ch_tasks (id,ora_sid) values (s_ora_to_ch_tasks.nextval,sys_context('USERENV','SID'))"
                   val idCol: Array[String] = Array("ID")
                   val insertTask           = conn.prepareStatement(query, idCol)
                   val affectedRows: Int    = insertTask.executeUpdate()
                   val generatedKeys        = insertTask.getGeneratedKeys
                   generatedKeys.next()
                   val taskId: Int          = generatedKeys.getInt(1)
                   conn.commit()
                   conn.setClientInfo("OCSID.ACTION", s"taskid_$taskId")
                   oraSessTask(conn, taskId)
                 }.tapError(er => ZIO.logError(er.getMessage))
                   .refineToOrDie[SQLException]
    sess      <- sessEffect
    md         = sess.sess.getMetaData
    sid       <- ZIO.attemptBlockingInterrupt {
                   sess.getPid
                 }.tapError(er => ZIO.logError(er.getMessage))
                   .refineToOrDie[SQLException]
    _         <- ZIO.logInfo(
                   s"Oracle DriverVersion : ${md.getJDBCMajorVersion}.${md.getJDBCMinorVersion} SID = $sid"
                 )
  } yield sess

  private def oraConnectionCalc(vqId: Int): ZIO[Any, SQLException, oraSessCalc] = for {
    oraConn <- oraRef.getConnection().refineToOrDie[SQLException]
    sess    <- ZIO.attemptBlockingInterrupt {
                 val conn = oraConn
                 conn.setAutoCommit(false)
                 conn.setClientInfo("OCSID.MODULE", "ORATOCH")
                 conn.setClientInfo("OCSID.ACTION", s"unknown")
                 oraSessCalc(conn, vqId)
               }.tapError(er => ZIO.logError(er.getMessage))
                 .refineToOrDie[SQLException]
  } yield sess

}

object jdbcSessionImpl {

  val layer: ZLayer[OraConnRepoImpl with SessTypeEnum with OraServer, SQLException, jdbcSession] =
    ZLayer {
      for {
        sessType <- ZIO.service[SessTypeEnum]
        conn     <- ZIO.service[OraConnRepoImpl]
      } yield jdbcSessionImpl(conn, sessType)
    }

}
