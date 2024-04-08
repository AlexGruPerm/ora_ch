package connrepo

import conf.OraServer
import oracle.jdbc.OracleDriver
import request.Parallel
import zio.{Ref, Scope, ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.Properties

/**
 *   1. Service Definition Service that contain single Oracle session. Only this one single session
 *      using during Task or Calc.
 */
trait OraConnRepo {
  def getConnection(): ZIO[Any, Throwable, Connection]
  def getUrl(): ZIO[Any, Nothing, String]
}

/**
 * 2. Service Implementation 3. Service Dependencies - we put its dependencies into its constructor.
 * All the dependencies are just interfaces, not implementation.
 */
final case class OraConnRepoImpl(conf: OraServer, ref: Ref[List[Connection]]) extends OraConnRepo {
  def getConnection(): ZIO[Any, Throwable, Connection] = for {
    r <- ref.get
    conn = r.find(_.isClosed == false) match {
      case Some(value) => value
      case None => throw new SQLException("no available connection.")
    }
  } yield conn

/*    ref.get.map(_.find(_.isClosed==false) match {
    case Some(c: Connection) => c
    case None => ZIO.die(new Throwable("No available connection."))
  })*/


  def getUrl(): ZIO[Any, Nothing, String]                 = ZIO.succeed(conf.getUrl())
}

/**
 * 4. ZLayer (Constructor)
 */
object OraConnRepoImpl {

  private def acquire(conf: OraServer, degree: Int): ZIO[Any, Exception, List[Connection]] = for {
    _          <- ZIO.logInfo("new oracle connection")
    connection <- ZIO.attemptBlockingInterrupt {
                    DriverManager.registerDriver(new OracleDriver())
                    val props = new Properties()
                    props.setProperty("user", conf.user)
                    props.setProperty("password", conf.password)
                    val connections: List[Connection] = (1 to degree).map {i =>
                      val conn = DriverManager.getConnection(conf.getUrl(), props)
                      conn.setAutoCommit(false)
                      conn.setClientInfo("OCSID.MODULE", "ORATOCH")
                      conn.setClientInfo("OCSID.ACTION", s"unknown_$i")
                      conn
                    }.toList
                   connections
                  }.catchAll { case e: Exception =>
                    ZIO.logError(e.getMessage) *>
                      ZIO.fail(
                        new Exception(s"createConnection : ${e.getMessage} - ${conf.getUrl()}")
                      )
                  }
  } yield connection

  private def release(listConn: => List[Connection]): ZIO[Any, Nothing, Unit] =
    ZIO.succeedBlocking(listConn.foreach(_.close()))

  private def source(conf: OraServer, degree: Int): ZIO[Scope, Exception, List[Connection]] =
    ZIO.acquireRelease(acquire(conf,degree))(release(_))

  def layer(conf: OraServer, degree: Int): ZLayer[Any, Exception, OraConnRepoImpl] =
    ZLayer.scoped(source(conf,degree).flatMap(conn => Ref.make(conn).map(r => OraConnRepoImpl(conf, r))))

}
