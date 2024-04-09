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

/*

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
  def getUrl(): ZIO[Any, Nothing, String] = ZIO.succeed(conf.getUrl())
}

/**
 * 4. ZLayer (Constructor)
 */
object OraConnRepoImpl {

  private def acquire(conf: OraServer, degree: Int): ZIO[Any, Exception, List[Connection]] = for {
    _          <- ZIO.logInfo(s"ORA CONN REPO, CREATE NEW CONNECTIONS degree = $degree.")
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
                          val rs :ResultSet = conn.createStatement.executeQuery("select distinct sid from v$mystat")
                          rs.next()
                          val sid = rs.getInt("sid")
                          rs.close()
                          println(s"New connection created - $i with sid = $sid")
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
    ZLayer.scoped(source(conf,degree)
      .flatMap(listConnections => Ref.make(listConnections)
        .map(r => OraConnRepoImpl(conf, r))))

}

 */
