package calc

import clickhouse.jdbcChSession
import common.{CalcState, Executing, Wait}
import ora.jdbcSession
import zio.{FiberId, ZIO}

import java.sql.ResultSet

/**
 * Contains functions that related with process of calculation in clickhouse.
*/
object CalcLogic {

  def debugInterruption(funcName: String): Set[FiberId] => ZIO[Any,Nothing,Unit] = (fibers: Set[FiberId]) =>
    for {
      fn <- ZIO.fiberId.map(_.threadName)
      _ <- ZIO.debug(
        s"The $fn fiber which is the underlying fiber of the $funcName task " +
          s"interrupted by ${fibers.map(_.threadName).mkString(", ")}"
      )
    } yield ()

  def getCalcMeta(reqCalc: ReqCalcSrc): ZIO[jdbcSession,Throwable,ViewQueryMeta] = for {
    oraSession <- ZIO.service[jdbcSession]
    ora <- oraSession.sess("getCalcMeta")
    _ <- ZIO.logInfo(s"getCalcMeta Oracle SID = ${ora.getPid}")
    vqMeta <- ora.getVqMeta(reqCalc.view_query_id)
  } yield vqMeta

  private def truncTable(tableName: String): ZIO[jdbcChSession, Throwable, Unit] = for {
    chSession <- ZIO.service[jdbcChSession]
    ch <- chSession.sess(0) //todo: 0 - just for debug, there is no task when we calc.
    _ <- ch.truncateTable(tableName)
  } yield ()

  private def insertFromQuery(meta: ViewQueryMeta, reqCalc: ReqCalcSrc): ZIO[jdbcSession with jdbcChSession, Throwable, Unit] = for {
    chSession <- ZIO.service[jdbcChSession]
    ch <- chSession.sess(0) //todo: 0 - just for debug, there is no task when we calc.
    _ <- ch.insertFromQuery(meta,reqCalc.params)
  } yield ()

  private def calcView(meta: ViewQueryMeta, reqCalc: ReqCalcSrc): ZIO[jdbcSession with jdbcChSession, Throwable, Unit] = for {
    _ <- ZIO.logInfo(s"calcView for vqId = ${reqCalc.view_query_id} clickhouse table = ${meta.chTable}")
    _ <- truncTable(meta.chTable)
  } yield ()

  private def calcQuery(meta: ViewQueryMeta, reqCalc: ReqCalcSrc): ZIO[jdbcSession with jdbcChSession, Throwable, Unit] = for {
    _ <- ZIO.logInfo(s"calcQuery for vqId = ${reqCalc.view_query_id} clickhouse table = ${meta.chTable}")
    _ <- truncTable(meta.chTable)
    _ <- insertFromQuery(meta, reqCalc)
  } yield ()

  /**
   * 1. get meta data about this calculation from oracle database.
   * 2.
  */
  def startCalculation(reqCalc: ReqCalcSrc, meta: ViewQueryMeta): ZIO[ImplCalcRepo with jdbcSession with jdbcChSession, Throwable, Unit] = for {
    _ <- ZIO.logInfo("startCalculation.............................")
    _ <- ZIO.logDebug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    _ <- ZIO.logDebug(s"view Name = ${meta.viewName}")
    _ <- ZIO.logDebug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    _ <- ZIO.logDebug(meta.query.getOrElse(" -no query- "))
    _ <- ZIO.logDebug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    _ <- ZIO.logInfo(s"ch_table = ${meta.chTable} params_count = ${meta.params.size}")
    repo <- ZIO.service[ImplCalcRepo]
    calc = ReqCalc(id = reqCalc.view_query_id,
      state = CalcState(Executing),
      oraServer = Some(reqCalc.servers.oracle),
      clickhouseServer = Some(reqCalc.servers.clickhouse),
      params = reqCalc.params)
    _ <- repo.create(calc)
    stateBefore <- repo.getState
    _ <- repo.setState(CalcState(Executing))
    stateAfter <- repo.getState
    calcId <- repo.getCalcId
    _ <- ZIO.logDebug(s"[startCalc] [$calcId] State: ${stateBefore.state} -> ${stateAfter.state} ")
    _ <- meta.viewName match {
      case Some(_) => calcView(meta, reqCalc)
      case None => calcQuery(meta, reqCalc)
    }
  } yield ()

  def copyDataChOra(reqCalc: ReqCalcSrc, meta: ViewQueryMeta): ZIO[ImplCalcRepo with jdbcSession with jdbcChSession, Throwable, Unit] = for {
    repo <- ZIO.service[ImplCalcRepo]
    stateBefore <- repo.getState
    _ <- ZIO.logInfo(s"Begin copyDataChOra ${reqCalc.view_query_id} ${meta.chTable}")
    chSession <- ZIO.service[jdbcChSession]
    ch <- chSession.sess(0) //todo: 0 - just for debug, there is no task when we calc.
    chTableRs <- ch.getChTableResultSet(meta.chTable)
    oraSession <- ZIO.service[jdbcSession]
    ora <- oraSession.sess("copyDataChOra")
    _ <- ora.insertRsDataInTable(chTableRs,meta.oraTable)
    _ <- repo.setState(CalcState(Wait))
    stateAfter <- repo.getState
    _ <- ZIO.logInfo(s"End copyDataChOra for ${reqCalc.view_query_id} State: ${stateBefore.state} -> ${stateAfter.state}")
  } yield ()

}