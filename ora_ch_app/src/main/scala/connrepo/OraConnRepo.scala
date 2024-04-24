package connrepo

import conf.OraServer
import org.apache.commons.dbcp2.BasicDataSource
import zio.{ Ref, Scope, ZIO, ZLayer }

import java.sql.Connection

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
final case class OraConnRepoImpl(conf: OraServer, ref: Ref[BasicDataSource]) extends OraConnRepo {
  def getConnection(): ZIO[Any, Throwable, Connection] = for {
    r <- ref.get
  } yield r.getConnection
  def getUrl(): ZIO[Any, Nothing, String]              = ZIO.succeed(conf.getUrl())
}

/**
 * 4. ZLayer (Constructor)
 */
object OraConnRepoImpl {
  private def acquire(conf: OraServer, degree: Int): ZIO[Any, Exception, BasicDataSource] = for {
    pool <- ZIO.attemptBlockingInterrupt {
              val dbUrl          = s"jdbc:oracle:thin:@//${conf.ip}:${conf.port}/${conf.tnsname}"
              val connectionPool = new BasicDataSource()
              connectionPool.setUsername(conf.user)
              connectionPool.setPassword(conf.password)
              connectionPool.setDriverClassName("oracle.jdbc.driver.OracleDriver")
              connectionPool.setUrl(dbUrl)
              connectionPool.setInitialSize(degree)
              connectionPool
            }.catchAll { case e: Exception =>
              ZIO.logError(e.getMessage) *>
                ZIO.fail(
                  new Exception(s"Create connection pool : ${e.getMessage} - ${conf.getUrl()}")
                )
            }
  } yield pool

  private def release(pool: => BasicDataSource): ZIO[Any, Nothing, Unit] =
    ZIO.succeedBlocking(pool.close())

  private def source(conf: OraServer, degree: Int): ZIO[Scope, Exception, BasicDataSource] =
    ZIO.acquireRelease(acquire(conf, degree))(release(_))

  def layer(conf: OraServer, degree: Int): ZLayer[Any, Exception, OraConnRepoImpl] =
    ZLayer.scoped(
      source(conf, degree)
        .flatMap(listConnections =>
          Ref
            .make(listConnections)
            .map(r => OraConnRepoImpl(conf, r))
        )
    )

}