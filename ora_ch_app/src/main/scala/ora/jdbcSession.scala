package ora

import zio.{Task, _}
import conf.OraServer
import jdk.internal.org.jline.utils.ShutdownHooks.Task
import oracle.jdbc.OracleDriver
import request.SrcTable
import table.{ExtPrimaryKey, KeyType, PrimaryKey, RnKey, Table, UniqueKey}

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties


case class oraSess(sess : Connection, taskId: Int){

  def getDataResultSet(table: Table, fetch_size: Int): ZIO[Any, Exception, ResultSet] = for {
    _ <- ZIO.unit
    rsEffect = ZIO.attemptBlocking {
      val dataQuery =
        table.keyType match {
          case ExtPrimaryKey => s"select * from ${table.schema}.${table.name}"
          case PrimaryKey | UniqueKey => s"select * from ${table.schema}.${table.name}" //todo: REMOVE where rownum <= 3000
          case RnKey => s"select row_number() over(order by null) as rn,t.* from ${table.schema}.${table.name} t" //todo: REMOVE where rownum <= 3000
        }
      //********************* CONTEXT *************************
      val ctxDate: String = table.plsql_context_date.getOrElse(" ")
      if (table.plsql_context_date.nonEmpty){
        //execute immediate 'ALTER SESSION SET nls_length_semantics = BYTE';
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
      val dateQueryWithOrd: String = table.ins_select_order_by match {
        case Some(order_by) => s"$dataQuery order by $order_by"
        case None => dataQuery
      }
      val dataRs = sess.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE)
        .executeQuery(dateQueryWithOrd)
      dataRs.setFetchSize(fetch_size)
      dataRs
    }.catchAll {
      case e: Exception => ZIO.logError(e.getMessage) *>
        ZIO.fail(new Exception(s"${e.getMessage}"))
    }
    rs <- rsEffect
  } yield rs


  def saveTableList(tables: List[Table]): ZIO[Any,Exception,Unit] = for {
    taskId <- getTaskIdFromSess
    saveTablesEffects = tables.map{t =>
      ZIO.attemptBlocking {
        val query: String =
          s"insert into ora_to_ch_tasks_tables(id_task,schema_name,table_name) values($taskId,'${t.schema}','${t.name}') "
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        rs.next() //todo: ??? maybe remove
        sess.commit()
        rs.close()
      }.catchAll {
        case e: Exception => ZIO.logError(e.getMessage) *>
          ZIO.fail(new Exception(s"${e.getMessage}"))
      }
    }
    _ <- ZIO.collectAll(saveTablesEffects)
  } yield ()

  def setTaskState(state: String): ZIO[Any, Exception, Unit] = for {
    taskId <- getTaskIdFromSess
    _ <-
      ZIO.attemptBlocking {
        val query: String =
          s"update ora_to_ch_tasks set state='$state' where id=$taskId"
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        rs.next() //todo: ??? maybe remove
        sess.commit()
        rs.close()
      }.catchAll {
        case e: Exception => ZIO.logError(e.getMessage) *>
          ZIO.fail(new Exception(s"${e.getMessage}"))
      }
  } yield ()

  def setTableBeginCopy(table: Table): ZIO[Any, Exception, Unit] = for {
    taskId <- getTaskIdFromSess
    _ <-
      ZIO.attemptBlocking {
        val query: String =
          s""" update ora_to_ch_tasks_tables t
             |   set  begin_datetime = sysdate,
             |        state  = 'copying'
             | where t.id_task=$taskId and
             |       t.schema_name='${table.schema}' and
             |       t.table_name='${table.name}' """.stripMargin
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        rs.next() //todo: ??? maybe remove
        sess.commit()
        rs.close()
      }.catchAll {
        case e: Exception => ZIO.logError(e.getMessage) *>
          ZIO.fail(new Exception(s"${e.getMessage}"))
      }
  } yield ()

  def updateCountCopiedRows(table: Table, rowCount: Int): ZIO[Any, Nothing, Unit] = for {
    taskId <- getTaskIdFromSess.catchAll {
      _: Exception => ZIO.succeed(0)
    }
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
        rs.next() //todo: ??? maybe remove
        sess.commit()
        rs.close()
        }.catchAllDefect {
           case e: Exception => ZIO.logError(s"No problem. updateCountCopiedRows - Defect - ${e.getMessage}")
        }
        .catchAll {
          case e: Exception =>ZIO.logError(s"No problem. updateCountCopiedRows - Defect - ${e.getMessage}")
        }
  } yield ()

  def setTableCopied(table: Table, rowCount: Int): ZIO[Any, Exception, Unit] = for {
    taskId <- getTaskIdFromSess
    _ <-
      ZIO.attemptBlocking {
        val query: String =
          s""" update ora_to_ch_tasks_tables t
             |   set end_datetime = sysdate,
             |             state  = 'finished',
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
        rs.next() //todo: ??? maybe remove
        sess.commit()
        rs.close()
      }.catchAll {
        case e: Exception => ZIO.logError(e.getMessage) *>
          ZIO.fail(new Exception(s"${e.getMessage}"))
      }
  } yield ()

  def taskFinished: ZIO[Any, Exception, Unit] = for {
    taskId <- getTaskIdFromSess
    _ <-
      ZIO.attemptBlocking {
        val query: String =
          s"update ora_to_ch_tasks set end_datetime = sysdate,state='finished' where id=$taskId"
        val rs: ResultSet = sess.createStatement.executeQuery(query)
        rs.next() //todo: ??? maybe remove
        sess.commit()
        rs.close()
      }.catchAll {
        case e: Exception => ZIO.logError(e.getMessage) *>
          ZIO.fail(new Exception(s"${e.getMessage}"))
      }
  } yield ()
  
  def closeConnection: ZIO[Any, Nothing, Unit] = for {
    _ <- ZIO.logInfo("Closing Oracle connection")
    _ <- ZIO.attemptBlocking {
        sess.close()
      }.catchAll {
        case e: Exception => ZIO.logError(s"Closing Oracle connection - ${e.getMessage}").unit //*>
          //ZIO.fail(new Exception(s"${e.getMessage}"))
      }
  } yield ()

  def getPid: Int = {
    val stmt: Statement = sess.createStatement
    val rs: ResultSet = stmt.executeQuery("select sys_context('USERENV','SID') as sid from dual")
    /*"select distinct sid from v$mystat"*/
    rs.next()
    val pg_backend_pid: Int = rs.getInt("sid")
    pg_backend_pid
  }

  def getKeyType(schema: String, tableName: String): ZIO[Any,Exception,(KeyType,String)] = for {
    _ <- ZIO.unit
    keysEffect = ZIO.attemptBlocking {
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
    }.catchAll {
      case e: Exception => ZIO.logError(e.getMessage) *>
        ZIO.fail(new Exception(s"${e.getMessage}"))
    }

    key <- keysEffect
    _ <- ZIO.logInfo(s" For $schema.$tableName KEY = ${key._1} - ${key._2}")
  } yield key

  def getTable(schema: String,
               tableName: String,
               plsql_context_date: Option[String],
               pk_columns: Option[String],
               ins_select_order_by: Option[String],
               partition_by: Option[String],
               notnull_columns: Option[List[String]]
              ): ZIO[Any, Exception, Table] = for {
    kt <- pk_columns match {
      case Some(ext_pk_cols) => ZIO.succeed((ExtPrimaryKey,ext_pk_cols))
      case None => getKeyType(schema, tableName)
    }
  } yield Table(schema, tableName, kt._1, kt._2, plsql_context_date, pk_columns, ins_select_order_by, partition_by, notnull_columns)

  def getTables(stables: List[SrcTable]): ZIO[Any, Exception, List[Table]] = {
    //todo: refactor tuple
    val schTbl: List[(String, String, Option[String], Option[String], Option[String], Option[String], Option[List[String]])] =
      stables.flatMap { st =>
      st.tables.map(ot =>
        (st.schema, ot.name, ot.plsql_context_date, ot.pk_columns, ot.ins_select_order_by, ot.partition_by, ot.notnull_columns))
    }

    val res: List[ZIO[Any, Exception, Table]] =
      for {
        t <- schTbl
      } yield getTable(t._1, t._2, t._3, t._4, t._5, t._6, t._7) //todo: refactor tuple

    val outTables = for {
      values <- ZIO.collectAll {
        res
      }
    } yield values

    outTables
  }

  def getTaskIdFromSess: ZIO[Any, Exception, Int] = ZIO.succeed(taskId)

}

trait jdbcSession {
  val sess: ZIO[Any,Exception,oraSess]
  val props = new Properties()
  def pgConnection(): ZIO[Any,Exception,oraSess]
}

case class jdbcSessionImpl(ora: OraServer) extends jdbcSession {

   val sess: ZIO[Any,Exception,oraSess] = for {
     session <- pgConnection()
   } yield session

   def pgConnection():  ZIO[Any,Exception,oraSess] = for {
    _ <- ZIO.unit
    sessEffect = ZIO.attemptBlocking{
        DriverManager.registerDriver(new OracleDriver())
        props.setProperty("user", ora.user)
        props.setProperty("password", ora.password)
        val conn = DriverManager.getConnection(ora.getUrl(), props)
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
        oraSess(conn,taskId)
      }.catchAll {
      case e: Exception => ZIO.logError(e.getMessage) *>
        ZIO.fail(new Exception(s"${e.getMessage} - ${ora.getUrl()}"))
    }

    _ <- ZIO.logInfo(s"  ") *>
      ZIO.logInfo(s"New connection =============== >>>>>>>>>>>>> ")
    sess <- sessEffect
    md = sess.sess.getMetaData
    _ <- ZIO.logInfo(s"Oracle DriverVersion : ${md.getJDBCMajorVersion}.${md.getJDBCMinorVersion}")
    _ <- ZIO.logInfo(s"oracle session id = ${sess.getPid}")

  } yield sess

}

object jdbcSessionImpl {

  val layer: ZLayer[OraServer, Exception, jdbcSession] =
    ZLayer{
      for {
        ora <- ZIO.service[OraServer]
      } yield jdbcSessionImpl(ora)
    }

}