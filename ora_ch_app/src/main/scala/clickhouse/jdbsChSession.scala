package clickhouse

import com.clickhouse.jdbc.{ClickHouseConnection, ClickHouseDataSource}
import conf.ClickhouseServer
import zio.{ZIO, ZLayer}

import java.sql.Connection
import java.util.Properties

case class chSess(sess : Connection, taskId: Int){

}

trait jdbcChSession {
  def sess(taskId: Int): ZIO[Any,Exception,chSess]
  val props = new Properties()
  def chConnection(taskId: Int): ZIO[Any,Exception,chSess]
}

case class jdbcSessionImpl(ch: ClickhouseServer) extends jdbcChSession {

  def sess(taskId: Int): ZIO[Any, Exception, chSess] = for {
    session <- chConnection(taskId)
  } yield session

  override def chConnection(taskId: Int): ZIO[Any, Exception, chSess] = for {
    _ <- ZIO.unit
    sessEffect = ZIO.attemptBlocking {
      props.setProperty("http_connection_provider", "HTTP_URL_CONNECTION")
      val dataSource = new ClickHouseDataSource(ch.getUrl, props)
      val conn: ClickHouseConnection = dataSource.getConnection("admin", "admin")
      chSess(conn,taskId)
    }.catchAll {
      case e: Exception => ZIO.logError(e.getMessage) *>
        ZIO.fail(new Exception(s"${e.getMessage} - ${ch.getUrl}"))
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