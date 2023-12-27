package ora

import zio.{Task, _}
import conf.OraServer

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties


case class oraSess(sess : Connection, taskId: Int){

  def getPid: Int = {
    val stmt: Statement = sess.createStatement
    val rs: ResultSet = stmt.executeQuery("SELECT pg_backend_pid() as pg_backend_pid")
    rs.next()
    val pg_backend_pid: Int = rs.getInt("pg_backend_pid")
    pg_backend_pid
  }

}

trait jdbcSession {
  val props = new Properties()
  def pgConnection(): ZIO[Any,Exception,oraSess]
}

case class jdbcSessionImpl(ora: OraServer) extends jdbcSession {

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
    _ <- ZIO.logInfo(s"pg_backend_pid = ${sess.getPid}")

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