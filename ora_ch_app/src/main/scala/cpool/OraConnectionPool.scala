package cpool

import conf.OraServer
import oracle.ucp.admin.{ UniversalConnectionPoolManager, UniversalConnectionPoolManagerImpl }
import oracle.ucp.jdbc.{ PoolDataSourceFactory, ValidConnection }
import request.Parallel

/**
 * Universal Connection Pool Developer's Guide
 * https://docs.oracle.com/en/database/oracle/oracle-database/12.2/jjucp/toc.htm Also read 4.3 About
 * Optimizing Real-World Performance with Static Connection Pools
 */
class OraConnectionPool(conf: OraServer, par: Parallel) {
  println(s" ******** OraConnectionPool CONSTRUCTOR with - ${par.degree} **********")
  val pool = PoolDataSourceFactory.getPoolDataSource
  pool.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource")
  pool.setURL(s"jdbc:oracle:thin:@//${conf.ip}:${conf.port}/${conf.tnsname}")
  pool.setUser(conf.user)
  pool.setPassword(conf.password)
  pool.setConnectionPoolName("Ucp" /*props.ConnectionPoolName*/ )
  pool.setConnectionWaitTimeout(30 /*props.ConnectionWaitTimeout*/ )
  pool.setInitialPoolSize(1 /*props.InitialPoolSize*/ )
  // A connection pool always tries to return to the minimum pool size
  pool.setMinPoolSize(1 /*props.MinPoolSize*/ )
  // The maximum pool size property specifies the maximum number of available
  // and borrowed (in use) connections that a pool maintains.
  pool.setMaxPoolSize(par.degree /*props.MaxPoolSize*/ )
  pool.setAbandonedConnectionTimeout(0 /*props.AbandonConnectionTimeout*/ )
  pool.setInactiveConnectionTimeout(Integer.MAX_VALUE /*props.InactiveConnectionTimeout*/ )
  // todo: check it.
  // https://docs.oracle.com/en/database/oracle/oracle-database/12.2/jjucp/validating-ucp-connections.html#GUID-A7C850D6-4026-4629-BCFA-9181C29EFBF9
  // pool.setValidateConnectionOnBorrow(true)

  val jdbcVersion: String = {
    val conn: java.sql.Connection     = pool.getConnection
    conn.setClientInfo("OCSID.MODULE", "ORATOCH")
    val md: java.sql.DatabaseMetaData = conn.getMetaData
    val jdbcVers: String              = s"${md.getJDBCMajorVersion}.${md.getJDBCMinorVersion}"
    conn.close()
    jdbcVers
  }

  def closePoolConnections: Unit = {
    // println(s"closePoolConnections pool.getAvailableConnectionsCount = ${pool.getAvailableConnectionsCount} ")
    // println(s"closePoolConnections pool.getBorrowedConnectionsCount  = ${pool.getBorrowedConnectionsCount} ")
    val mgr: UniversalConnectionPoolManager =
      UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager
    // mgr.stopConnectionPool("Ucp")
    (1 to pool.getAvailableConnectionsCount + pool.getBorrowedConnectionsCount).foreach { _ =>
      val c = pool.getConnection()
      c.asInstanceOf[ValidConnection].setInvalid()
      c.close()
    }
    mgr.destroyConnectionPool("Ucp")
  }

}
