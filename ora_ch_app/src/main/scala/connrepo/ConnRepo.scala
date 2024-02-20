package connrepo

import conf.OraServer
import oracle.jdbc.OracleDriver
import zio.{ Ref, Scope, ZIO, ZLayer }

import java.sql.{ Connection, DriverManager, ResultSet, SQLException }
import java.util.Properties

/**
 *   1. Service Definition Service that contain single Oracle session. Only this one single session
 *      using during Task or Calc.
 */
trait OraConnRepo {
  def getConnection(): ZIO[Any, SQLException, Connection]
  def getUrl(): ZIO[Any, Nothing, String]
}

/**
 * 2. Service Implementation 3. Service Dependencies - we put its dependencies into its constructor.
 * All the dependencies are just interfaces, not implementation.
 */
case class OraConnRepoImpl(conf: OraServer, ref: Ref[Connection]) extends OraConnRepo {
  def getConnection(): ZIO[Any, SQLException, Connection] = ref.get
  def getUrl(): ZIO[Any, Nothing, String]                 = ZIO.succeed(conf.getUrl())
}

/**
 * 4. ZLayer (Constructor)
 */
object OraConnRepoImpl {

  private def acquire(conf: OraServer): ZIO[Any, Exception, Connection] = for {
    _          <- ZIO.logInfo(
                    s"OraConnRepoImpl.acquire CREATE NEW ORACLE CONNECTION >>>>>>>>>>>>>>>>>>>>>>>>>>>>"
                  )
    connection <- ZIO.attemptBlockingInterrupt {
                    DriverManager.registerDriver(new OracleDriver())
                    val props = new Properties()
                    props.setProperty("user", conf.user)
                    props.setProperty("password", conf.password)
                    val conn  = DriverManager.getConnection(conf.getUrl(), props)
                    conn.setAutoCommit(false)
                    conn.setClientInfo("OCSID.MODULE", "ORATOCH")
                    conn.setClientInfo("OCSID.ACTION", s"unknown")
                    conn
                  }.catchAll { case e: Exception =>
                    ZIO.logError(e.getMessage) *>
                      ZIO.fail(
                        new Exception(s"createConnection : ${e.getMessage} - ${conf.getUrl()}")
                      )
                  }
  } yield connection

  private def release(conn: => Connection): ZIO[Any, Nothing, Unit] =
    ZIO.succeedBlocking(conn.close())

  private def source(conf: OraServer): ZIO[Scope, Exception, Connection] =
    ZIO.acquireRelease(acquire(conf))(release(_))

  def layer(conf: OraServer): ZLayer[Any, Exception, OraConnRepoImpl] =
    ZLayer.scoped(source(conf).flatMap(conn => Ref.make(conn).map(r => OraConnRepoImpl(conf, r))))

}
