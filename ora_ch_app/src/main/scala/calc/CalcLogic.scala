package calc

import clickhouse.jdbcChSession
import common.{CalcState, Calculation, Copying, Wait}
import ora.{jdbcSession, oraSessCalc}
import zio.{FiberId, ZIO}

import java.sql.SQLException

/**
 * Contains functions that related with process of calculation in clickhouse.
 */
object CalcLogic {

  def getCalcMeta(ora: oraSessCalc, reqCalc: ReqCalcSrc): ZIO[Any, Throwable, ViewQueryMeta] = for {
    _          <- ZIO.logInfo(s"getCalcMeta Oracle SID = ${ora.getPid}")
    vqMeta     <- ora.getVqMeta(reqCalc.view_query_id)
  } yield vqMeta

  private def truncTable(meta: ViewQueryMeta): ZIO[jdbcChSession, Throwable, Unit] = for {
    chSession <- ZIO.service[jdbcChSession]
    ch        <- chSession.sess(0)
    _         <- ch.truncateTable(meta)
  } yield ()

  private def insertFromQuery(
    meta: ViewQueryMeta,
    reqCalc: ReqCalcSrc
  ): ZIO[/*jdbcSession with*/ jdbcChSession, Throwable, Unit] = for {
    chSession <- ZIO.service[jdbcChSession]
    ch        <- chSession.sess(0)
    _         <- ch.insertFromQuery(meta, reqCalc.params)
  } yield ()

  private def calcView(
    meta: ViewQueryMeta,
    reqCalc: ReqCalcSrc
  ): ZIO[jdbcChSession, Throwable, Unit] = for {
    _ <- ZIO.logInfo(
           s"calcView for vqId = ${reqCalc.view_query_id} clickhouse table = ${meta.chTable}"
         )
    _ <- truncTable(meta)
    // _ <- insertFromView(meta, reqCalc)
  } yield ()

  private def calcQuery(
    meta: ViewQueryMeta,
    reqCalc: ReqCalcSrc
  ): ZIO[/*jdbcSession with*/ jdbcChSession, Throwable, Unit] = for {
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
  private def beginQueryLog(ora: oraSessCalc, idVq: Int): ZIO[Any/*jdbcSession*/, Throwable, Int] = for {
    //oraSession <- ZIO.service[jdbcSession]
    //ora        <- oraSession.sessCalc(debugMsg = "beginQueryLog")
    id         <- ora.insertViewQueryLog(idVq)
  } yield id

  private def saveEndCalculation(ora: oraSessCalc): ZIO[ImplCalcRepo /*with jdbcSession*/, Throwable, Unit] = for {
    repo       <- ZIO.service[ImplCalcRepo]
    calcId     <- repo.getCalcId
    //oraSession <- ZIO.service[jdbcSession]
    //ora        <- oraSession.sessCalc(debugMsg = s"saveEndCalculation for calcId=$calcId")
    _          <- ora.saveEndCalculation(calcId)
  } yield ()

  private def saveEndCopying(ora: oraSessCalc): ZIO[ImplCalcRepo /*with jdbcSession*/, Throwable, Unit] = for {
    repo       <- ZIO.service[ImplCalcRepo]
    calcId     <- repo.getCalcId
    //oraSession <- ZIO.service[jdbcSession]
    //ora        <- oraSession.sessCalc(debugMsg = "saveEndCopying")
    _          <- ora.saveEndCopying(calcId)
    _          <- repo.setState(CalcState(Wait))
    _          <- repo.clearCalc
  } yield ()

  private def saveCalcError(
    ora: oraSessCalc,
    errorMsg: String,
    calcId: Int
  ): ZIO[ImplCalcRepo /*with jdbcSession*/, Throwable, Unit] =
    for {
      _          <- ZIO.logInfo(s"> saveCalcError calcId = $calcId")
      repo       <- ZIO.service[ImplCalcRepo]
      //oraSession <- ZIO.service[jdbcSession]
      //ora        <- oraSession.sessCalc(debugMsg = "copyDataChOra")
      _          <- ora.saveCalcError(calcId, errorMsg)
      _          <- repo.setState(CalcState(Wait))
      _          <- repo.clearCalc
    } yield ()

  def getOraConnFromPool(): ZIO[jdbcSession, SQLException, oraSessCalc] = for {
    oraSession <- ZIO.service[jdbcSession]
    ora <- oraSession.sessCalc(debugMsg = "getCalcMeta")
  } yield ora


  /**
   *   1. get meta data about this calculation from oracle database. 2.
   */
  def startCalculation(
    ora: oraSessCalc,
    reqCalc: ReqCalcSrc,
    meta: ViewQueryMeta
  ): ZIO[ImplCalcRepo /*with jdbcSession*/ with jdbcChSession, Throwable, Unit] = for {
    _           <- ZIO.logDebug("startCalculation.............................")
    _           <-
      ZIO.logDebug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    _           <- ZIO.logDebug(s"view Name = ${meta.viewName}")
    _           <-
      ZIO.logDebug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    _           <- ZIO.logDebug(meta.query.getOrElse(" -no query- "))
    _           <-
      ZIO.logDebug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    _           <- ZIO.logInfo(s"ch_table = ${meta.chTable} params_count = ${meta.params.size}")
    repo        <- ZIO.service[ImplCalcRepo]
    calc         = ReqCalc(
                     id = 0,
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
    id          <- beginQueryLog(ora,calc.id_vq)
    _           <-
      ZIO.logDebug(s"[startCalc] calcId=[$id] State: ${stateBefore.state} -> ${stateAfter.state} ")
    _           <- repo.updateCalcId(id)
    calcEffect   = meta.viewName match {
                     case Some(_) => calcView(meta, reqCalc)
                     case None    => calcQuery(meta, reqCalc)
                   }
    _           <- calcEffect.tapError(er =>
                     ZIO.logError(s" startCalculation - calcEffect ${er.getMessage}") *>
                       saveCalcError(ora,er.getMessage, id)
                   )
    _           <- saveEndCalculation(ora)
  } yield ()

  def copyDataChOra(
    ora: oraSessCalc,
    reqCalc: ReqCalcSrc,
    meta: ViewQueryMeta
  ): ZIO[ImplCalcRepo /*with jdbcSession*/ with jdbcChSession, Throwable, Unit] = for {
    repo        <- ZIO.service[ImplCalcRepo]
    stateBefore <- repo.getState
    _           <- ZIO.logInfo(s"Begin copyDataChOra ${reqCalc.view_query_id} ${meta.chTable}")
    chSession   <- ZIO.service[jdbcChSession]
    ch          <- chSession.sess(0)
    chTableRs   <- ch.getChTableResultSet(meta)
    //oraSession  <- ZIO.service[jdbcSession]
    //ora         <- oraSession.sessCalc(debugMsg = "copyDataChOra")
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
    _           <- saveEndCopying(ora)
    _           <-
      ZIO.logInfo(
        s"End copyDataChOra for ${reqCalc.view_query_id} State: ${stateBefore.state} -> ${stateAfter.state}"
      )
  } yield ()

}
