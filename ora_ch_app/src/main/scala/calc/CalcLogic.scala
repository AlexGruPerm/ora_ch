package calc

import clickhouse.jdbcChSession
import common.{CalcState, Calculation, Copying, Wait}
import ora.{jdbcSession, oraSessCalc}
import request.Servers
import zio.ZIO

/**
 * Contains functions that related with process of calculation in clickhouse.
 */
object CalcLogic {

  def getCalcMeta(query: Query, ora: oraSessCalc): ZIO[Any, Throwable, ViewQueryMeta] = for {
    _ <- ZIO.logInfo(s"getCalcMeta for query_id = ${query.query_id}")
    meta <- ora.getQueryMeta(query.query_id)
  } yield meta

  private def truncTable(meta: ViewQueryMeta): ZIO[jdbcChSession, Throwable, Unit] = for {
    ch <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    _  <- ch.truncateTable(meta)
  } yield ()

  private def insertFromQuery(
    meta: ViewQueryMeta,
    query: Query
  ): ZIO[jdbcChSession, Throwable, Unit] = for {
    ch <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    _  <- ch.insertFromQuery(meta, query.params)
  } yield ()

  private def calcQuery(
    meta: ViewQueryMeta,
    query: Query
  ): ZIO[jdbcChSession, Throwable, Unit] = for {
    _ <- ZIO.logInfo(
           s"calcQuery for query_id = ${query.query_id} clickhouse table = ${meta.chTable}"
         )
    _ <- truncTable(meta)
    _ <- insertFromQuery(meta, query)
  } yield ()



/*  private def saveEndCalculation(): ZIO[ImplCalcRepo with jdbcSession, Throwable, Unit] = for {
    calcId <- ZIO.serviceWithZIO[ImplCalcRepo](_.getCalcId)
    _ <- ZIO.logInfo(s"saveEndCalculation for calcId = $calcId")
    ora    <- ZIO.serviceWithZIO[jdbcSession](_.sessCalc(debugMsg = "calc - saveEndCalculation"))
    _      <- ora.saveEndCalculation(calcId)
  } yield ()*/

/*  private def saveCalcError(
    errorMsg: String,
    calcId: Int,
    ora: oraSessCalc
  ): ZIO[Any/*ImplCalcRepo with jdbcSession*/, Throwable, Unit] =
    for {
      _    <- ZIO.logInfo(s"saveCalcError calcId = $calcId")
      //repo <- ZIO.service[ImplCalcRepo]
      //ora  <- ZIO.serviceWithZIO[jdbcSession](_.sessCalc(debugMsg = "calc saveCalcError"))
      _    <- ora.saveCalcError(calcId, errorMsg)
      //_    <- repo.setState(CalcState(Wait))
      //_    <- repo.clearCalc
    } yield ()*/

  /**
   *   1. get meta data about this calculation from oracle database. 2.
   */
  def startCalculation(
                        query: Query,
                        //servers: Servers,
                        meta: ViewQueryMeta,
                        ora: oraSessCalc,
                        id_reload_calc: Int
  ): ZIO[/*ImplCalcRepo with jdbcSession with*/ jdbcChSession, Throwable, Int] = for {
    _           <- ZIO.logDebug("startCalculation")
    // _           <- ZIO.logDebug(meta.query.getOrElse(" -no query- "))
    _           <- ZIO.logInfo(s"ch_table = ${meta.chTable} params_count = ${meta.params.size}")
  /*  repo        <- ZIO.service[ImplCalcRepo]
    calc         = ReqCalc(
                     id_vq = query.query_id,
                     state = CalcState(Wait),
                     oraServer = Some(servers.oracle),
                     clickhouseServer = Some(servers.clickhouse),
                     params = query.params
                   )
    _           <- repo.create(calc)
    stateBefore <- repo.getState
    _           <- repo.setState(CalcState(Calculation))
    stateAfter  <- repo.getState*/

    queryLogId  <- ora.insertViewQueryLog(query,id_reload_calc)
    /*_           <-
      ZIO.logDebug(s"[startCalc] calcId=[$queryLogId] State: ${stateBefore.state} -> ${stateAfter.state} ")
    _           <- repo.updateCalcId(queryLogId)*/
    _           <- calcQuery(meta, query).tapError(er =>
                     ZIO.logError(s" startCalculation - calcEffect ${er.getMessage}") *>
                       //saveCalcError(er.getMessage, queryLogId, ora)
                       ora.saveCalcError(queryLogId, er.getMessage)
                   )
    _      <- ora.saveEndCalculation(queryLogId)
  } yield queryLogId

  def copyDataChOra(
                     query: Query,
                     meta: ViewQueryMeta,
                     ora: oraSessCalc,
                     queryLogId: Int
                   ): ZIO[/*ImplCalcRepo with jdbcSession with*/ jdbcChSession, Throwable, Unit] = for {
    //repo        <- ZIO.service[ImplCalcRepo]
    //stateBefore <- repo.getState
    _           <- ZIO.logInfo(s"Begin copyDataChOra for query_id = ${query.query_id} ${meta.chTable}")
    ch          <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    chTableRs   <- ch.getChTableResultSet(meta)
    //ora         <- ZIO.serviceWithZIO[jdbcSession](_.sessCalc(debugMsg = "calc - copyDataChOra"))
    //_           <- repo.setState(CalcState(Copying))
    //calcId      <- repo.getCalcId
    _           <- ora.saveBeginCopying(queryLogId)
    _           <- ora.truncateTable(meta.oraSchema, meta.oraTable)
    _           <- ora
                     .insertRsDataInTable(chTableRs, meta.oraTable, meta.oraSchema)
                     .tapError(er =>
                       ZIO.logError(s"insertRsDataInTable - ${er.getMessage}") //*>
                         //repo.setState(CalcState(Wait))
                     )
    //_           <- repo.setState(CalcState(Wait))
    //stateAfter  <- repo.getState
    _           <- ora.saveEndCopying(queryLogId)
    //_           <- repo.setState(CalcState(Wait))
    //_           <- repo.clearCalc
    _           <-
      ZIO.logInfo(
        s"End copyDataChOra for query_id = ${query.query_id}"
      )
  } yield ()

}
