package ora

import calc.{VQParams, ViewQueryMeta}
import column.OraChColumn
import zio.{Task, ZIO, _}
import conf.OraServer
import oracle.jdbc.OracleDriver
import request.SrcTable
import table.{ExtPrimaryKey, KeyType, PrimaryKey, RnKey, Table, UniqueKey}

import java.sql.{Clob, Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import java.util.Properties
import common.Types._
import OraChColumn._
import common.{ResultSetWithQuery, SessCalc, SessTask, SessTypeEnum}
import connrepo.OraConnRepoImpl

sealed trait oraSess {
  def sess: Connection
  //def closeConnection: ZIO[Any, Nothing, Unit]
  def getPid: Int = {
    val stmt: Statement = sess.createStatement
    val rs: ResultSet = stmt.executeQuery("select sys_context('USERENV','SID') as sid from dual")
    rs.next()
    val pg_backend_pid: Int = rs.getInt("sid")
    rs.close()
    pg_backend_pid
  }
}

case class oraSessCalc(sess : Connection, calcId: Int) extends oraSess {

  def insertViewQueryLog(idVq: Int): ZIO[Any, Throwable, Int] = for {
    id <- ZIO.attemptBlocking {
      val query: String =
        s""" insert into ora_to_ch_views_query_log(id,id_vq,ora_sid,begin_calc,state)
           | values(s_ora_to_ch_views_query_log.nextval, $idVq, sys_context('USERENV','SID'), sysdate, 'calculation')
           | """.stripMargin
      val idCol: Array[String] = Array("ID")
      val insertVqLog = sess.prepareStatement(query, idCol)
      val affectedRows: Int = insertVqLog.executeUpdate()
      val generatedKeys = insertVqLog.getGeneratedKeys
      generatedKeys.next()
      val id: Int = generatedKeys.getInt(1)
      sess.commit()
      sess.setClientInfo("OCSID.MODULE", "ORATOCH")
      sess.setClientInfo("OCSID.ACTION", s"calc_$id")
      sess.commit()
      insertVqLog.close()
      id
    }.tapError(er => ZIO.logError(er.getMessage))
    _ <- ZIO.logInfo(s"AFTER insertViewQueryLog id=$id")
  } yield id

  def saveEndCalculation(id: Int): ZIO[Any, Throwable, Unit] = for {
    _ <- ZIO.logInfo(s"saveEndCalculation for calcId = $calcId id=$id")
    _ <- ZIO.attemptBlocking {
      val query: String =
        s""" update ora_to_ch_views_query_log l
           |   set l.end_calc = sysdate,
           |       l.state    = 'copying'
           | where l.id = $id
           | """.stripMargin
      val rs: ResultSet = sess.createStatement.executeQuery(query)
      rs.next()
      sess.commit()
      rs.close()
    }.tapError(er => ZIO.logError(er.getMessage))
  } yield ()

  def saveBeginCopying(id: Int): ZIO[Any, Throwable, Unit] = for {
    _ <- ZIO.attemptBlocking {
      val query: String =
        s""" update ora_to_ch_views_query_log l
           |   set l.begin_copy = sysdate,
           |       l.state    = 'copying'
           | where l.id = $id
           | """.stripMargin
      val rs: ResultSet = sess.createStatement.executeQuery(query)
      sess.commit()
      rs.close()
    }.tapError(er => ZIO.logError(er.getMessage))
  } yield ()

  def saveEndCopying(id: Int): ZIO[Any, Throwable, Unit] = for {
    _ <- ZIO.attemptBlocking {
      val query: String =
        s""" update ora_to_ch_views_query_log l
           |   set l.end_copy = sysdate,
           |       l.state    = 'finished'
           | where l.id = $id
           | """.stripMargin
      val rs: ResultSet = sess.createStatement.executeQuery(query)
      sess.commit()
      rs.close()
    }.tapError(er => ZIO.logError(er.getMessage))
  } yield ()

  def getVqMeta(vqId: Int): ZIO[Any, Throwable, ViewQueryMeta] = for {
    _ <- ZIO.unit
    vqMeta <- ZIO.attemptBlockingInterrupt {
      val queryVq =
        s""" select vq.view_name,vq.ch_table,vq.ora_table,vq.query_text
           |  from ora_to_ch_views_query vq
           | where vq.id = $vqId """.stripMargin
      val rsVq: ResultSet = sess.createStatement.executeQuery(queryVq)
      rsVq.next()

      val queryParams =
        s""" select p.param_name,p.param_type,p.param_order
           |  from ora_to_ch_vq_params p
           | where p.id_vq = $vqId """.stripMargin
      val rsParams: ResultSet = sess.createStatement.executeQuery(queryParams)
      val vqParams: Set[VQParams] = Iterator.continually(rsParams).takeWhile(_.next()).map { rs =>
        VQParams(
          rs.getString("param_name"),
          rs.getString("param_type"),
          rs.getInt("param_order")
        )
      }.toSet

      val vName: String = rsVq.getString("view_name")
      val isNameNull: Boolean = rsVq.wasNull()
      val optVName: Option[String] =
        if (isNameNull)
          None
        else
          Some(vName)

      val queryClob: Clob = rsVq.getClob("query_text")
      val isNull: Boolean = rsVq.wasNull()
      val optQueryStr: Option[String] =
        if (isNull)
          None
        else
          Some(queryClob.getSubString(1, queryClob.length().toInt))

      val vqm = ViewQueryMeta(
        optVName,
        rsVq.getString("ch_table"),
        rsVq.getString("ora_table"),
        optQueryStr,
        vqParams
      )
      rsVq.close()
      rsParams.close()
      vqm
    }.tapError(er => ZIO.logError(er.getMessage))
  } yield vqMeta

  private def debugRsColumns(rs: ResultSet): ZIO[Any, Nothing, Unit] = for {
        _ <- ZIO.logDebug(s"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
       _ <- ZIO.foreachDiscard((1 to rs.getMetaData.getColumnCount)) { i =>
         ZIO.logDebug(
           s"""${rs.getMetaData.getColumnName(i).toLowerCase} -
              |${rs.getMetaData.getColumnTypeName(i)} -
              |${rs.getMetaData.getColumnClassName(i)} -
              |${rs.getMetaData.getColumnDisplaySize(i)} -
              |${rs.getMetaData.getPrecision(i)} -
              |${rs.getMetaData.getScale(i)}
              |""".stripMargin)
       }
       _ <- ZIO.logDebug(s"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  } yield ()

  def getColumnsFromRs(rs: ResultSet): ZIO[Any, Throwable, List[OraChColumn]] = for {
    _ <- debugRsColumns(rs)
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
          )).toList
    }.tapError(er => ZIO.logError(er.getMessage))
  } yield cols

  def insertRsDataInTable(rs: ResultSet, tableName: String): ZIO[Any, Throwable, Unit] = for {
    cols <- getColumnsFromRs(rs).tapError(er => ZIO.logError(er.getMessage))
    _ <- ZIO.attemptBlockingInterrupt {
      val batch_size = 1000
      val fetch_size = 1000
      val nakedCols = cols.map(chCol => chCol.name).mkString(",")
      val bindQuests = cols.map(_ => "?").mkString(",")
      val insQuery: String = s"insert into msk_analytics_caches.$tableName($nakedCols) values($bindQuests)"
      //println(s" insQuery = $insQuery")

      val ps: PreparedStatement = sess.prepareStatement(insQuery)
      rs.setFetchSize(fetch_size)

      Iterator.continually(rs).takeWhile(_.next()).foldLeft(1) {
        case (counter, rs) =>
          cols.foldLeft(1) {
            case (i, c) =>
              (c.typeName, c.typeClass, c.precision) match {
                case ("UInt8" | "UInt16" | "UInt32" | "UInt64", _, _) => ps.setLong(i, rs.getLong(c.name))
                case (_, "java.math.BigDecimal", _) => ps.setDouble(i, rs.getDouble(c.name))
                case (_, "java.lang.String", _) => ps.setString(i, rs.getString(c.name))
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
    }.tapError(er => ZIO.logError(er.getMessage))
  } yield ()

}

case class oraSessTask(sess : Connection, taskId: Int) extends oraSess{

  private def setContext(table: Table): Unit =
    if (table.plsql_context_date.nonEmpty) {
      val ctxDate: String = table.plsql_context_date.getOrElse(" ")
      val contextSql =
        s"""
           | begin
           |   msk_analytics.set_curr_date_context(to_char(to_date($ctxDate,'yyyymmdd'),'dd.mm.yyyy'));
           |   DBMS_SESSION.SET_CONTEXT('CLIENTCONTEXT','ANALYT_DATECALC',to_char(to_date($ctxDate,'yyyymmdd'),'dd.mm.yyyy'));
           |end;
           |""".stripMargin
      val prep = sess.prepareCall(contextSql)
      prep.execute()
    } else ()

  private def getAppendByFieldsPart(table: Table,
                                    appendKeys: Option[List[Any]] = Option.empty[List[Any]]): String = {
    val appendKeysList: List[Any] = appendKeys.getOrElse(List.empty[Any])
    val filterStr = s" ( ${table.sync_by_columns.getOrElse(" EMPTY_SYNC_BY_COLUMNS ")} ) "
    val filterTuples: String =
    if (table.syncArity() == 1){
      s"(${appendKeysList.mkString(",")})"
    } else if (table.syncArity() == 2){
      s"""(${appendKeysList.map(lst =>
        s"(${lst.asInstanceOf[(Int,Int)].productIterator.toList.mkString(",")})").mkString(",")})"""
    } else if (table.syncArity() == 3){
      s"""(${appendKeysList.map(lst =>
        s"(${lst.asInstanceOf[(Int,Int,Int)].productIterator.toList.mkString(",")})").mkString(",")})"""
    } else
       " "
    s" $filterStr not in $filterTuples "
  }

  def getDataResultSet(table: Table,
                       fetch_size: Int,
                       maxColCh: Option[MaxValAndCnt],
                       appendKeys: Option[List[Any]] = Option.empty[List[Any]]):
  ZIO[Any, Throwable, ResultSet] = for {
    _ <- ZIO.logDebug(
      s"recreateTableCopyData arity = ${table.syncArity()} appendKeys.nonEmpty = ${appendKeys.nonEmpty}")
    rsWithQuery <- ZIO.attemptBlocking {
      val whereByFields: String = getAppendByFieldsPart(table,appendKeys)
      val dataQuery =
        s""" ${
               table.keyType match {
                 case RnKey => " select row_number() over(order by null) as rn,t.* from ( "
                 case _     => " "
               }
              }
            select ${table.only_columns.getOrElse("*")} from ${table.schema}.${table.name} ${
          (table.where_filter, table.sync_by_column_max, table.sync_by_columns) match {
            case (Some(where), Some(syncCol), None) => s" where $where and $syncCol > ${maxColCh.map(_.MaxValue).getOrElse(0L)} "
            case (Some(where), None, Some(_))       => s" where $where and $whereByFields "
            case (_, None, Some(_))                 => s" where $whereByFields "
            case (_, Some(syncCol), None)           => s" where $syncCol > ${maxColCh.map(_.MaxValue).getOrElse(0L)} "
            case (Some(where), _, _)                => s" where $where "
            case _ => " "
          }
        } ${
          table.keyType match {
            case RnKey => " ) t "
            case _ => " "
          }
        }
          ${table.ins_select_order_by.map(order => s" order by $order ").getOrElse(" ")}
        """.stripMargin

      setContext(table)
      val dataRs = sess.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY)
        .executeQuery(dataQuery)
      dataRs.setFetchSize(fetch_size)
      ResultSetWithQuery(dataRs,dataQuery)
    }.tapBoth(er => ZIO.logError(er.getMessage),
      rsq => ZIO.logDebug(s"getDataResultSet select = ${rsq.query} "))
  } yield rsWithQuery.rs

  /**
   * Get oracle dataset for update clickhouse table.
   * Select part contains only primary key columns plus update_fields.
   * In this method we only use next Json keys:
   * name - table for update fields in clickhouse.
   * update_fields - fields that will updated in clickhouse table from oracle data.
   * where_filter - filter when select from oracle table.
   * ins_select_order_by
  */
  def getDataResultSetForUpdate(table: Table, fetch_size: Int, pkColumns: List[String]):
  ZIO[Any, Throwable, ResultSet] = for {
    _ <- ZIO.unit
    selectColumns = s"${pkColumns.mkString(",")},${table.updateColumns()} "
    rs <- ZIO.attemptBlocking {
      val dataQuery = {
        table.keyType match {
          case PrimaryKey => s"select $selectColumns from ${table.fullTableName()} ${table.whereFilter()}"
          case _ => throw new Exception("Update fields on table without primary key not possible.")
        }
      }
      println(s"getDataResultSetForUpdate dataQuery = $dataQuery")
      //********************* CONTEXT *************************
      val ctxDate: String = table.plsql_context_date.getOrElse(" ")
      if (table.plsql_context_date.nonEmpty) {
        val contextSql =
          s"""
             | begin
             |   msk_analytics.set_curr_date_context(to_char(to_date($ctxDate,'yyyymmdd'),'dd.mm.yyyy'));
             |   DBMS_SESSION.SET_CONTEXT('CLIENTCONTEXT','ANALYT_DATECALC',to_char(to_date($ctxDate,'yyyymmdd'),'dd.mm.yyyy'));
             |end;
             |""".stripMargin
        val prec = sess.prepareCall(contextSql)
        prec.execute()
      } else ()
      //*******************************************************
      val dateQueryWithOrd: String = s"$dataQuery ${table.orderBy()}"

      val dataRs = sess.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        .executeQuery(dateQueryWithOrd)
      dataRs.setFetchSize(fetch_size)
      dataRs
    }.tapError(er => ZIO.logError(er.getMessage))
  } yield rs

  def saveTableList(tables: List[Table]): ZIO[Any, Throwable, Unit] = for {
    taskId <- getTaskIdFromSess
    saveTablesEffects = tables.map { t =>
      ZIO.attemptBlocking {
        val query: String =
          s"insert into ora_to_ch_tasks_tables(id_task,schema_name,table_name) values($taskId,'${t.schema}','${t.name}') "
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        sess.commit()
        rs.close()
      }.tapError(er => ZIO.logError(er.getMessage))
    }
    _ <- ZIO.collectAll(saveTablesEffects)
  } yield ()

  def setTaskState(state: String): ZIO[Any, Throwable, Unit] = for {
    taskId <- getTaskIdFromSess
    _ <-
      ZIO.attemptBlocking {
        val query: String =
          s"update ora_to_ch_tasks set state='$state' where id=$taskId"
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        sess.commit()
        rs.close()
      }.tapError(er => ZIO.logError(er.getMessage))
  } yield ()

  def setTaskError(error: String,table: Table): ZIO[Any, Throwable, Unit] = for {
    taskId <- getTaskIdFromSess
    _ <-
      ZIO.attemptBlocking {
        val rsTask: ResultSet = sess.createStatement.executeQuery(
          s"update ora_to_ch_tasks set state='error',end_datetime=sysdate, error_msg='$error' where id=$taskId "
        )
        sess.commit()
        rsTask.close()

        val rsTable: ResultSet = sess.createStatement.executeQuery(
          s"""update ora_to_ch_tasks_tables t
              |set state = 'error',
              |    end_datetime = sysdate
              | where t.id_task     = $taskId and
              |       t.schema_name = '${table.schema}' and
              |       t.table_name  = '${table.name}'
          """.stripMargin
        )
        sess.commit()
        rsTable.close()

      }.tapError(er => ZIO.logError(er.getMessage))
  } yield ()

  def setTableBeginCopy(table: Table): ZIO[Any, Throwable, Unit] = for {
    taskId <- getTaskIdFromSess
    _ <-
      ZIO.attemptBlocking {
        val query: String =
          s""" update ora_to_ch_tasks_tables t
             |   set  begin_datetime = sysdate,
             |        state  = 'copying'
             | where t.id_task     = $taskId and
             |       t.schema_name = '${table.schema}' and
             |       t.table_name  = '${table.name}' """.stripMargin
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        sess.commit()
        rs.close()
      }.tapError(er => ZIO.logError(er.getMessage))
  } yield ()

  def updateCountCopiedRows(table: Table, rowCount: Long): ZIO[Any, Nothing, Unit] = for {
    taskId <- getTaskIdFromSess orElse ZIO.succeed(0)
    _ <-
      ZIO.attemptBlocking {
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
             |       t.table_name  = '${table.name}' """.stripMargin
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        sess.commit()
        rs.close()
        }.tapError(er => ZIO.logError(er.getMessage))
        .tapDefect(df => ZIO.logError(df.toString)) orElse ZIO.unit //todo: change df.toString use Cause !
  } yield ()

  def setTableCopied(table: Table, rowCount: Long, status: String): ZIO[Any, Throwable, Unit] = for {
    taskId <- getTaskIdFromSess
    _ <-
      ZIO.attemptBlocking {
        val query: String =
          s""" update ora_to_ch_tasks_tables t
             |   set end_datetime = sysdate,
             |             state  = '$status',
             |             copied_records_count = $rowCount,
             |             speed_rows_sec = (case
             |                                when ((sysdate - t.begin_datetime)*24*60*60) != 0
             |                                then round($rowCount/((sysdate - t.begin_datetime)*24*60*60))
             |                                else $rowCount
             |                               end)
             | where t.id_task     = $taskId and
             |       t.schema_name = '${table.schema}' and
             |       t.table_name  = '${table.name}' """.stripMargin
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        sess.commit()
        rs.close()
      }.tapError(er => ZIO.logError(er.getMessage))
  } yield ()

  def taskFinished: ZIO[Any, Throwable, Unit] = for {
    taskId <- getTaskIdFromSess
    _ <- ZIO.attemptBlocking {
        val query: String =
          s"update ora_to_ch_tasks set end_datetime = sysdate, state='finished' where id = $taskId"
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        sess.commit()
        rs.close()
      }.tapError(er => ZIO.logError(er.getMessage))
  } yield ()

  private def getKeyType(schema: String, tableName: String): ZIO[Any,Throwable,(KeyType,String)] = for {
    _ <- ZIO.unit
    key <- ZIO.attemptBlocking {
      val query: String =
        s"""
           |with pk as (
           | select 'PrimaryKey' as keytype,
           |         listagg(cols.column_name,',') within group (order by cols.position) as keycolumns
           |   from all_constraints cons, all_cons_columns cols
           |  where cols.table_name = '${tableName.toUpperCase}'
           |    and cons.OWNER      = '${schema.toUpperCase}'
           |    and cons.constraint_type = 'P'
           |    and cons.constraint_name = cols.constraint_name
           |    and cons.owner = cols.owner
           |    and cons.status = 'ENABLED'
           |  ),
           | uk as (
           |      select 'UniqueKey' as keytype,
           |             listagg(column_name,',') within group (order by position) as keycolumns
           |       from(
           |          select cols.column_name,
           |                 cols.POSITION,
           |                 cols.CONSTRAINT_NAME,
           |                 dense_rank() over(partition by null order by cols.CONSTRAINT_NAME) as rn
           |          from  all_constraints cons, all_cons_columns cols
           |          where cols.table_name = '${tableName.toUpperCase}'
           |            and cons.OWNER      = '${schema.toUpperCase}'
           |            and cons.constraint_type = 'U'
           |            and cons.constraint_name = cols.constraint_name
           |            and cons.owner = cols.owner
           |            and cons.status = 'ENABLED'
           |       ) where rn=1
           |           and not exists(select 1 from pk where pk.keycolumns is not null)
           | ),
           | rnk as (
           |         select 'RnKey' as keytype,
           |                'rn'    as keycolumns
           |           from dual
           |          where not exists(select 1 from uk where uk.keycolumns is not null) and
           |                not exists(select 1 from pk where pk.keycolumns is not null)
           |        )
           | select * from pk where pk.keycolumns is not null union all
           | select * from uk where uk.keycolumns is not null union all
           | select * from rnk
           |""".stripMargin
      val rs: ResultSet = sess.createStatement.executeQuery(query)
      rs.next()

      val keyType: KeyType = rs.getString("KEYTYPE").toLowerCase match {
        case "primarykey" => PrimaryKey
        case "uniquekey"  => UniqueKey
        case "rnkey"      => RnKey
      }
      val keyColumns: String = rs.getString("KEYCOLUMNS").toLowerCase

      rs.close()
      (keyType,keyColumns)
    }.tapError(er => ZIO.logError(er.getMessage))
    _ <- ZIO.logInfo(s" For $schema.$tableName KEY = ${key._1} - ${key._2}")
  } yield key

  private def getPk(schema: String,
                    tableName: String,
                    pk_columns: Option[String]): ZIO[Any, Throwable, (KeyType,String)] = for {
    ktc <- pk_columns match {
      case Some(ext_pk_cols) => ZIO.succeed((ExtPrimaryKey, ext_pk_cols))
      case None => getKeyType(schema, tableName)
    }
  } yield ktc

  def getTables(stables: List[SrcTable]): ZIO[Any, Throwable, List[Table]] =
    ZIO.collectAll {
      stables.flatMap { st =>
        st.tables.map{ot =>
          getPk(st.schema,ot.name,ot.pk_columns).map{
            case (kt: KeyType,kc: String) =>
              Table(
                st.schema,
                ot.recreate,ot.name,
                kt, kc,
                ot.plsql_context_date,
                ot.pk_columns,
                ot.only_columns,
                ot.ins_select_order_by,
                ot.partition_by,
                ot.notnull_columns,
                ot.where_filter,
                ot.sync_by_column_max,
                ot.update_fields,
                ot.sync_by_columns)
          }
        }
      }
    }

  def getTaskIdFromSess: ZIO[Any, Exception, Int] = ZIO.succeed(taskId)

}

trait jdbcSession {
  def sessTask(debugMsg: String): ZIO[Any,Throwable,oraSessTask]
  def sessCalc(vqId: Int = 0, debugMsg: String): ZIO[Any,Throwable,oraSessCalc]
  val props = new Properties()
}

case class jdbcSessionImpl(oraRef: OraConnRepoImpl, sessType: SessTypeEnum) extends jdbcSession {

   println(" ")
   println(s"###### This is a jdbcSessionImpl main constructor sessType = $sessType  ########")
   println(" ")

   def sessTask(debugMsg: String): ZIO[Any,Throwable,oraSessTask] = for {
     _ <- ZIO.logInfo(s"[sessTask] jdbcSessionImpl.sess [$debugMsg] call oraConnectionTask")
     session <- oraConnectionTask()
   } yield session

  def sessCalc(vqId: Int, debugMsg: String): ZIO[Any, Throwable, oraSessCalc] = for {
    _ <- ZIO.logInfo(s"[sessCalc] jdbcSessionImpl.sess [$debugMsg] call oraConnectionCalc")
    session <- oraConnectionCalc(vqId)
  } yield session

  private def oraConnectionTask():  ZIO[Any,Throwable,oraSessTask] = for {
    _ <- ZIO.unit
    oraConn <- oraRef.getConnection()
    sessEffect = ZIO.attemptBlocking{
        val conn = oraConn
        conn.setAutoCommit(false)
        conn.setClientInfo("OCSID.MODULE", "ORATOCH")
        val query: String = "insert into ora_to_ch_tasks (id,ora_sid) values (s_ora_to_ch_tasks.nextval,sys_context('USERENV','SID'))"
        val idCol: Array[String] = Array("ID")
        val insertTask = conn.prepareStatement(query, idCol)
        val affectedRows: Int = insertTask.executeUpdate()
        val generatedKeys = insertTask.getGeneratedKeys
        generatedKeys.next()
        val taskId: Int = generatedKeys.getInt(1)
        conn.commit()
        conn.setClientInfo("OCSID.ACTION", s"taskid_$taskId")
        oraSessTask(conn,taskId)
      }.tapError(er => ZIO.logError(er.getMessage))
    sess <- sessEffect
    md = sess.sess.getMetaData
    sid <- ZIO.attemptBlocking {
      sess.getPid
    }.tapError(er => ZIO.logError(er.getMessage))
    _ <- ZIO.logInfo(s"Oracle DriverVersion : ${md.getJDBCMajorVersion}.${md.getJDBCMinorVersion} SID = $sid")
  } yield sess

  private def oraConnectionCalc(vqId: Int): ZIO[Any, Throwable, oraSessCalc] = for {
    oraConn <- oraRef.getConnection()
    sessEffect = ZIO.attemptBlocking {
      val conn = oraConn
      conn.setAutoCommit(false)
      conn.setClientInfo("OCSID.MODULE", "ORATOCH")
      conn.setClientInfo("OCSID.ACTION", s"unknown")
      oraSessCalc(conn, vqId)
    }.tapError(er => ZIO.logError(er.getMessage))
    sess <- sessEffect
    md = sess.sess.getMetaData
    sid <- ZIO.attemptBlocking {
      sess.getPid
    }.tapError(er => ZIO.logError(er.getMessage))
    _ <- ZIO.logInfo(s"Oracle DriverVersion : ${md.getJDBCMajorVersion}.${md.getJDBCMinorVersion} SID = $sid")
  } yield sess

}

object jdbcSessionImpl {

  val layer: ZLayer[OraConnRepoImpl with SessTypeEnum with OraServer, Exception, jdbcSession] =
    ZLayer{
      for {
        sessType <- ZIO.service[SessTypeEnum]
        conn <- ZIO.service[OraConnRepoImpl]
      } yield jdbcSessionImpl(conn,sessType)
    }

}