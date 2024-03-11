package connrepo

import conf.OraServer
import cpool.OraConnectionPool
import oracle.jdbc.OracleDriver
import request.Parallel
import zio.{ Ref, Scope, ZIO, ZLayer }

import java.sql.{ Connection, DriverManager, ResultSet, SQLException }
import java.util.Properties

/**
 *   1. Service Definition Service that contain single Oracle session. Only this one single session
 *      using during Task or Calc.
 */
trait OraConnRepo {
  def getConnection(setModule: Option[String] = Option.empty): ZIO[Any, SQLException, Connection]
  def setConnectionPoolName(name: String): ZIO[Any, SQLException, Unit]
  def getConnectionPoolName: ZIO[Any, SQLException, Unit]
  def getMaxPoolSize: ZIO[Any, SQLException, Int]
  def closeAll(poolName: String): ZIO[Any, SQLException, Unit]
  def getJdbcVersion: ZIO[Any, SQLException, String]
  def getUrl(): ZIO[Any, Nothing, String]
}

/**
 * 2. Service Implementation 3. Service Dependencies - we put its dependencies into its constructor.
 * All the dependencies are just interfaces, not implementation.
 */
final case class OraConnRepoImpl(conf: OraServer, ref: Ref[OraConnectionPool]) extends OraConnRepo {

  /**
   * mps <- oraRef.getMaxPoolSize pn <- oraRef.getConnectionPoolName
   */
  override def getConnection(
    setModule: Option[String] = Option.empty
  ): ZIO[Any, SQLException, Connection] = for {
    _         <- ZIO.logInfo(">>>>>>>>>>>>>>>>>> getConnection in OraConnRepoImpl")
    refPool   <- ref.get
    _         <-
      ZIO.logInfo(
        s"PoolName = ${refPool.pool.getConnectionPoolName} MaxPoolSize = ${refPool.pool.getMaxPoolSize}"
      )
    connection = refPool.pool.getConnection()
  } yield connection

  override def setConnectionPoolName(name: String): ZIO[Any, SQLException, Unit] =
    ref.get.map(cp => cp.pool.setConnectionPoolName(name))

  override def getConnectionPoolName: ZIO[Any, SQLException, Unit] =
    ref.get.map(cp => cp.pool.getConnectionPoolName)

  override def getMaxPoolSize: ZIO[Any, SQLException, Int] =
    ref.get.map(cp => cp.pool.getMaxPoolSize)

  override def closeAll(poolName: String): ZIO[Any, SQLException, Unit] =
    ref.get.map(_.closePoolConnections(poolName))

  override def getJdbcVersion: ZIO[Any, SQLException, String] = ref.get.map(cp => cp.jdbcVersion)

  def getUrl(): ZIO[Any, Nothing, String] = ZIO.succeed(conf.getUrl())
}

/**
 * 4. ZLayer (Constructor)
 */
object OraConnRepoImpl {

  private def acquire(conf: OraServer, par: Parallel, poolName: String): ZIO[Any, Exception, OraConnectionPool] =
    for {
      _    <- ZIO.logInfo(s"acquire - new oracle connection pool with parallel = ${par.degree}")
      pool <- ZIO.attemptBlockingInterrupt {
                new OraConnectionPool(conf, par, poolName)
              }.catchAll { case e: Exception =>
                ZIO.logError(e.getMessage) *>
                  ZIO.fail(
                    new Exception(s"createConnection : ${e.getMessage} - ${conf.getUrl()}")
                  )
              }
    } yield pool

  private def release(pool: => OraConnectionPool,poolName: String): ZIO[Any, Throwable, Unit] = for {
    _ <- ZIO.logInfo("release - closing all connections.")
    _ <- ZIO.attemptBlockingInterrupt {
           pool.closePoolConnections(poolName)
         }
  } yield ()

  private def source(conf: OraServer, par: Parallel, poolName: String): ZIO[Scope, Exception, OraConnectionPool] =
    ZIO.acquireRelease(acquire(conf, par, poolName)) {pool =>
      release(pool, poolName).catchAll { e: Throwable =>
        ZIO.logError(s"release catchAll ${e.getMessage}")
      }
    }

  def layer(conf: OraServer, par: Parallel, poolName: String): ZLayer[Any, Exception, OraConnRepoImpl] =
    ZLayer.scoped(
      source(conf, par, poolName).flatMap(conn => Ref.make(conn).map(r => OraConnRepoImpl(conf, r)))
    )

}
