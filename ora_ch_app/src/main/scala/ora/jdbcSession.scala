package ora

import calc.{ Query, VQParams, ViewQueryMeta }
import zio._
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
import common.{ ResultSetWithQuery, SessCalc, SessTask, SessTypeEnum, UpdateStructsScripts }
import connrepo.OraConnRepoImpl

import java.util.concurrent.TimeUnit

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

  def saveEndCopying(id: Int, meta: ViewQueryMeta): ZIO[Any, SQLException, Unit] = for {
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
                |               cd.curr_date_context = to_number(replace(l.curr_date_context,'-',''))),
                |   l.copied_rows = (select sum(1) from ${meta.oraSchema}.${meta.oraTable})
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
                     s""" select vq.ch_table,vq.ora_table,vq.query_text,vq.ch_schema,vq.ora_schema,vq.copy_ch_ora_columns
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
                     rsVq.getString("ora_schema"),
                     rsVq.getString("copy_ch_ora_columns")
                   )
                   rsVq.close()
                   rsParams.close()
                   vqm
                 }.tapError(er => ZIO.logError(er.getMessage))
                   .refineToOrDie[SQLException]
  } yield queryMeta

}

case class oraSessTask(sess: Connection, taskId: Int) extends oraSess {

  def getCreateScript(table: Table): ZIO[Any, SQLException, String] = for {
    script <- ZIO.attemptBlockingInterrupt {
                val rs: ResultSet = sess
                  .createStatement()
                  .executeQuery(s"""
                                   |select t.create_ch_script
                                   |  from orach.ora_ch_data_tables_meta t
                                   | where t.operation  = 'recreate' and
                                   |       t.schema     = '${table.schema}' and
                                   |       t.table_name = '${table.name}'
                                   |""".stripMargin)
                rs.next()
                val sql           = rs.getString("create_ch_script")
                rs.close()
                sql
              }.tapError(er => ZIO.logError(er.getMessage))
                .refineToOrDie[SQLException]
  } yield script

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
    sid            <- ZIO.attemptBlockingInterrupt {
                        getPid
                      }.tapError(er => ZIO.logError(er.getMessage))
                        .refineToOrDie[SQLException]
    _              <-
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
    _              <-
      ZIO.logInfo(
        s"getDataResultSet maxColCh.CntRows = ${maxColCh.map(_.CntRows).getOrElse(0L)} fetch_size = $fetch_size"
      )
    dtBeforeOpenRs <- Clock.currentTime(TimeUnit.MILLISECONDS)
    rsWithQuery    <- ZIO.attemptBlockingInterrupt {
                        val whereByFields: String = getAppendByFieldsPart(table, appendKeys)
                        /// *+ ALL_ROWS */
                        val dataQuery             =
                          s""" select  ${table.only_columns.getOrElse("*")} from ${table
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
    dtAfterOpenRs  <- Clock.currentTime(TimeUnit.MILLISECONDS)
    _              <-
      ZIO.logInfo(s"ORACLE.RS Execute select query timing : ${dtAfterOpenRs - dtBeforeOpenRs} ms.")
    _              <- ZIO.logInfo(s"ORACLE getDataResultSet, QUERY = ${rsWithQuery.query}")
  } yield rsWithQuery.rs

  /**
   * Get oracle dataset for update clickhouse table. Select part contains only primary key columns
   * plus update_fields. In this method we only use next Json keys: name - table for update fields
   * in clickhouse. update_fields - fields that will updated in clickhouse table from oracle data.
   * where_filter - filter when select from oracle table. ins_select_order_by
   */
  /*  def getDataResultSetForUpdate(
    table: Table,
    fetch_size: Int,
    primaryKeyColumnsCh: List[String]
  ): ZIO[Any, SQLException, ResultSet] = for {
    _            <- ZIO.unit
    selectColumns = s"${primaryKeyColumnsCh.mkString(",")},${table.updateColumns()} "
    rs           <- ZIO.attemptBlockingInterrupt {
                      val dataQuery                =
                        s"select  $selectColumns from ${table.fullTableName()} ${table.whereFilter()}"
                      println(s"getDataResultSetForUpdate dataQuery = $dataQuery")
                      setContext(table)
                      //val dateQueryWithOrd: String = s"$dataQuery ${table.orderBy()}"
                      val dataRs                   = sess
                        .createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
                        .executeQuery(dataQuery)
                      dataRs.setFetchSize(fetch_size)
                      dataRs
                    }.tapError(er => ZIO.logError(er.getMessage))
                      .refineToOrDie[SQLException]
  } yield rs
   */

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

  def setTaskErrorSaveError(error: String, table: Table): ZIO[Any, SQLException, Unit] = for {
    taskId <- getTaskIdFromSess
    errMsg  = s"${table.fullTableName()} $error"
    _      <-
      ZIO.attemptBlockingInterrupt {
        val rsTask: ResultSet  = sess
          .createStatement
          .executeQuery(
            s"""update ora_to_ch_tasks
               |   set state        = 'error',
               |       end_datetime = sysdate,
               |       error_msg    = '${errMsg.take(4000)}'
               | where id = $taskId """.stripMargin
          )
        sess.commit()
        rsTask.close()
        val rsTable: ResultSet = sess
          .createStatement
          .executeQuery(
            s"""update ora_to_ch_tasks_tables t
               |set state = 'error',
               |    end_datetime     = sysdate
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

  def getUpdTblDictScripts(table: Table): ZIO[Any, SQLException, UpdateStructsScripts] = for {
    scripts <- ZIO.attemptBlockingInterrupt {
                 val rs: ResultSet = sess
                   .createStatement()
                   .executeQuery(s"""
                                    |select t.upd_table_script,
                                    |       t.dict_script_fupdate
                                    |  from orach.ora_ch_data_tables_meta t
                                    | where t.operation  = 'update' and
                                    |       t.schema     = '${table.schema}' and
                                    |       t.table_name = '${table.name}'
                                    |""".stripMargin)
                 rs.next()
                 val sqlScripts    = UpdateStructsScripts(
                   rs.getString("upd_table_script"),
                   rs.getString("dict_script_fupdate")
                 )
                 rs.close()
                 sqlScripts
               }.tapError(er => ZIO.logError(er.getMessage))
                 .refineToOrDie[SQLException]
  } yield scripts

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

  def updateCountCopiedRows(
    table: Table,
    rowCount: Long,
    oper: String
  ): ZIO[Any, SQLException, Long] =
    for {
      taskId <- getTaskIdFromSess
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
          sess.setClientInfo(
            "OCSID.ACTION",
            s"UCNT_$oper-$rowCount"
          ) // todo: check it for 32 symbols table name
          sess.commit()
          rs.close()
          rowCount
        }.tapError(er =>
          ZIO.logError(s"ERROR - updateCountCopiedRows [${table.name}]: ${er.getMessage}")
        ).tapDefect(df =>
          ZIO.logError(s"DEFECT - updateCountCopiedRows [${table.name}]: ${df.toString}")
        ) orElse
          ZIO.succeed(0L) // todo: remove orElse
    } yield rows

  def clearOraTable(clearTable: String): ZIO[Any, SQLException, Unit] =
    for {
      _ <-
        ZIO.attemptBlockingInterrupt {
          val query: String = s" delete from $clearTable "
          val rs: ResultSet = sess.createStatement.executeQuery(query)
          sess.commit()
          rs.close()
        }.refineToOrDie[SQLException]
    } yield ()

  def setTableCopied(table: Table, rowCount: Long): ZIO[Any, SQLException, Unit] =
    for {
      taskId <- getTaskIdFromSess
      _      <- ZIO.logInfo(s"setTableCopied debug ora sid = ${getPid}")
      _      <- ZIO.attemptBlockingInterrupt {
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
              // t.pk_columns,
              t.only_columns,
              // t.ins_select_order_by,
              // t.partition_by,
              // t.notnull_columns,
              t.where_filter,
              t.sync_by_column_max,
              t.update_fields,
              t.sync_by_columns,
              t.sync_update_by_column_max,
              t.clr_ora_table_aft_upd,
              t.order_by_ora_data
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

  def sessTask(taskIdOpt: Option[Int] = Option.empty): ZIO[Any, SQLException, oraSessTask] = for {
    _       <- ZIO.logInfo(
                 s"[sessTask] jdbcSessionImpl.sess [${taskIdOpt.getOrElse(0)}] call oraConnectionTask"
               )
    _       <- ZIO.logInfo(s"sessTask taskIdOpt = $taskIdOpt")
    session <- if (taskIdOpt.getOrElse(0) == 0)
                 oraConnectionTask().timeout(30.seconds)
               else
                 oraConnectionTaskEx(taskIdOpt).timeout(30.seconds)
    s       <- session match {
                 case Some(conn) => ZIO.succeed(conn)
                 case None       => ZIO.fail(new SQLException(s"Can not connect to oracle db. sessTask"))
               }
  } yield s

  def sessCalc(vqId: Int, debugMsg: String): ZIO[Any, SQLException, oraSessCalc] = for {
    _       <- ZIO.logDebug(s"[sessCalc] jdbcSessionImpl.sess [$debugMsg] call oraConnectionCalc")
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
    oraConn <- oraRef.getConnection().refineToOrDie[SQLException]

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
                 }.timeout(5.seconds)
                   .refineToOrDie[SQLException]
                   .tapError(er =>
                     ZIO.logError(
                       s"oraConnectionTask [timeout or different error] error: ${er.getMessage}"
                     )
                   )
    optS      <- sessEffect
    sessE      = optS match {
                   case Some(s) => ZIO.succeed(s)
                   case None    =>
                     ZIO.fail(new SQLException(s"[orach - 1]Can not connect to oracle db. sessTask"))
                 }
    sess      <- sessE
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
