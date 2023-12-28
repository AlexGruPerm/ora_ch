package ora

import zio.{Task, _}
import conf.OraServer
import request.SrcTable
import table.{KeyType, PrimaryKey, RnKey, Table, UniqueKey}

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties


case class oraSess(sess : Connection, taskId: Int){

  def getPid: Int = {
    val stmt: Statement = sess.createStatement
    val rs: ResultSet = stmt.executeQuery("select distinct sid from v$mystat")
    rs.next()
    val pg_backend_pid: Int = rs.getInt("sid")
    pg_backend_pid
  }

  def getKeyType(schema: String, tableName: String): ZIO[Any,Exception,(KeyType,String)] = for {
    _ <- ZIO.unit
    //todo: make single big query for oracle to get KeyType and KeyColumns
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

      (keyType,keyColumns)

    }.catchAll {
      case e: Exception => ZIO.logError(e.getMessage) *>
        ZIO.fail(new Exception(s"${e.getMessage}"))
    }

    key <- keysEffect
    _ <- ZIO.logInfo(s" For $schema.$tableName KEY = ${key._1} - ${key._2}")
  } yield key

  def getTable(schema: String, tableName: String): ZIO[Any, Exception, Table] = for {
    kt <- getKeyType(schema, tableName)
  } yield Table(schema, tableName, kt._1, kt._2)

  def getTables(stables: List[SrcTable]): ZIO[Any, Exception, List[Table]] = {
    val schTbl: List[(String, String)] = stables.flatMap { st =>
      st.tables.map(ot => (st.schema, ot.name))
    }

    val res: List[ZIO[Any, Exception, Table]] =
      for {
        t <- schTbl
      } yield getTable(t._1, t._2)

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

   override def pgConnection():  ZIO[Any,Exception,oraSess] = for {
    _ <- ZIO.unit
    sessEffect = ZIO.attemptBlocking{
        props.setProperty("user", ora.user)
        props.setProperty("password", ora.password)
        val conn = DriverManager.getConnection(ora.getUrl(), props)
        conn.setAutoCommit(true)
        conn.setClientInfo("OCSID.MODULE", "ORATOCH")
        val query: String = "insert into ora_to_ch_tasks (id) values (s_ora_to_ch_tasks.nextval)"
        val idCol: Array[String] = Array("ID")
        val insertTask = conn.prepareStatement(query, idCol)
        val affectedRows: Int = insertTask.executeUpdate()
        val generatedKeys = insertTask.getGeneratedKeys
        generatedKeys.next()
        val taskId: Int = generatedKeys.getInt(1)
        conn.setClientInfo("OCSID.ACTION", s"taskid_$taskId")
        oraSess(conn,taskId)
      }.catchAll {
      case e: Exception => ZIO.logError(e.getMessage) *>
        ZIO.fail(new Exception(s"${e.getMessage} - ${ora.getUrl()}"))
    }

    _ <- ZIO.logInfo(s"  ") *>
      ZIO.logInfo(s"New connection =============== >>>>>>>>>>>>> ")
    sess <- sessEffect
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