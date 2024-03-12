package calc

import clickhouse.jdbcChSession
import common.{ CalcState, Calculation, Copying, Wait }
import ora.jdbcSession
import zio.ZIO

/**
 * Contains functions that related with process of calculation in clickhouse.
 */
object CalcLogic {

  def getCalcMeta(reqCalc: ReqCalcSrc): ZIO[jdbcSession, Throwable, ViewQueryMeta] = for {
    ora  <- ZIO.serviceWithZIO[jdbcSession](_.sessCalc(debugMsg = "calc - copyDataChOra"))
    meta <- ora.getVqMeta(reqCalc.view_query_id)
  } yield meta

  private def truncTable(meta: ViewQueryMeta): ZIO[jdbcChSession, Throwable, Unit] = for {
    ch <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    _  <- ch.truncateTable(meta)
  } yield ()

  private def insertFromQuery(
    meta: ViewQueryMeta,
    reqCalc: ReqCalcSrc
  ): ZIO[jdbcChSession, Throwable, Unit] = for {
    ch <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    _  <- ch.insertFromQuery(meta, reqCalc.params)
  } yield ()

  private def calcView(
    meta: ViewQueryMeta,
    reqCalc: ReqCalcSrc
  ): ZIO[jdbcChSession, Throwable, Unit] = for {
    _ <- ZIO.logInfo(
           s"calcView for vqId = ${reqCalc.view_query_id} clickhouse table = ${meta.chTable}"
         )
    _ <- truncTable(meta)
  } yield ()

  private def calcQuery(
    meta: ViewQueryMeta,
    reqCalc: ReqCalcSrc
  ): ZIO[jdbcChSession, Throwable, Unit] = for {
    _ <- ZIO.logInfo(
           s"calcQuery for vqId = ${reqCalc.view_query_id} clickhouse table = ${meta.chTable}"
         )
    _ <- truncTable(meta)
    _ <- insertFromQuery(meta, reqCalc)
  } yield ()

  /**
   * Save new row into log table ora_to_ch_views_query_log for given ID_VQ (look
   * ora_to_ch_views_query)
   */
  private def beginQueryLog(idVq: Int): ZIO[jdbcSession, Throwable, Int] =
    for {
      ora <- ZIO.serviceWithZIO[jdbcSession](_.sessCalc(debugMsg = "calc - beginQueryLog"))
      id  <- ora.insertViewQueryLog(idVq)
    } yield id

  private def saveEndCalculation(): ZIO[ImplCalcRepo with jdbcSession, Throwable, Unit] = for {
    calcId <- ZIO.serviceWithZIO[ImplCalcRepo](_.getCalcId)
    ora    <- ZIO.serviceWithZIO[jdbcSession](_.sessCalc(debugMsg = "calc - saveEndCalculation"))
    _      <- ora.saveEndCalculation(calcId)
  } yield ()

  private def saveCalcError(
    errorMsg: String,
    calcId: Int
  ): ZIO[ImplCalcRepo with jdbcSession, Throwable, Unit] =
    for {
      _    <- ZIO.logInfo(s"saveCalcError calcId = $calcId")
      repo <- ZIO.service[ImplCalcRepo]
      ora  <- ZIO.serviceWithZIO[jdbcSession](_.sessCalc(debugMsg = "calc saveCalcError"))
      _    <- ora.saveCalcError(calcId, errorMsg)
      _    <- repo.setState(CalcState(Wait))
      _    <- repo.clearCalc
    } yield ()

  /**
   *   1. get meta data about this calculation from oracle database. 2.
   */
  def startCalculation(
    reqCalc: ReqCalcSrc,
    meta: ViewQueryMeta
  ): ZIO[ImplCalcRepo with jdbcSession with jdbcChSession, Throwable, Unit] = for {
    _           <- ZIO.logDebug("startCalculation")
    // _           <- ZIO.logDebug(meta.query.getOrElse(" -no query- "))
    _           <- ZIO.logInfo(s"ch_table = ${meta.chTable} params_count = ${meta.params.size}")
    repo        <- ZIO.service[ImplCalcRepo]
    calc         = ReqCalc(
                     id_vq = reqCalc.view_query_id,
                     state = CalcState(Wait),
                     oraServer = Some(reqCalc.servers.oracle),
                     clickhouseServer = Some(reqCalc.servers.clickhouse),
                     params = reqCalc.params
                   )
    _           <- repo.create(calc)
    stateBefore <- repo.getState
    _           <- repo.setState(CalcState(Calculation))
    stateAfter  <- repo.getState
    id          <- beginQueryLog(calc.id_vq)
    _           <-
      ZIO.logDebug(s"[startCalc] calcId=[$id] State: ${stateBefore.state} -> ${stateAfter.state} ")
    _           <- repo.updateCalcId(id)
    calcEffect   = meta.viewName match {
                     case Some(_) => calcView(meta, reqCalc)
                     case None    => calcQuery(meta, reqCalc)
                   }
    _           <- calcEffect.tapError(er =>
                     ZIO.logError(s" startCalculation - calcEffect ${er.getMessage}") *>
                       saveCalcError(er.getMessage, id)
                   )
    _           <- saveEndCalculation()
  } yield ()

  def copyDataChOra(
    reqCalc: ReqCalcSrc,
    meta: ViewQueryMeta
  ): ZIO[ImplCalcRepo with jdbcSession with jdbcChSession, Throwable, Unit] = for {
    repo        <- ZIO.service[ImplCalcRepo]
    stateBefore <- repo.getState
    _           <- ZIO.logInfo(s"Begin copyDataChOra ${reqCalc.view_query_id} ${meta.chTable}")
    ch          <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    chTableRs   <- ch.getChTableResultSet(meta)
    ora         <- ZIO.serviceWithZIO[jdbcSession](_.sessCalc(debugMsg = "calc - copyDataChOra"))
    _           <- repo.setState(CalcState(Copying))
    calcId      <- repo.getCalcId
    _           <- ora.saveBeginCopying(calcId)
    _           <- ora.truncateTable(meta.oraSchema, meta.oraTable)
    _           <- ora
                     .insertRsDataInTable(chTableRs, meta.oraTable, meta.oraSchema)
                     .tapError(er =>
                       ZIO.logError(er.getMessage) *>
                         repo.setState(CalcState(Wait))
                     )
    _           <- repo.setState(CalcState(Wait))
    stateAfter  <- repo.getState
    _           <- ora.saveEndCopying(calcId)
    _           <- repo.setState(CalcState(Wait))
    _           <- repo.clearCalc
    _           <-
      ZIO.logInfo(
        s"End copyDataChOra for ${reqCalc.view_query_id} State: ${stateBefore.state} -> ${stateAfter.state}"
      )
  } yield ()

}
