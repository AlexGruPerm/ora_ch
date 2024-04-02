package calc

import clickhouse.{ chSess, jdbcChSession }
import common.{ CalcState, Calculation, Copying, Wait }
import ora.{ jdbcSession, oraSessCalc }
import request.Servers
import zio.ZIO

/**
 * Contains functions that related with process of calculation in clickhouse.
 */
object CalcLogic {

  def getCalcMeta(query: Query, ora: oraSessCalc): ZIO[Any, Throwable, ViewQueryMeta] = for {
    _    <- ZIO.logInfo(s"getCalcMeta for query_id = ${query.query_id}")
    meta <- ora.getQueryMeta(query.query_id)
  } yield meta

  /*  private def truncTable(meta: ViewQueryMeta,ch: chSess): ZIO[jdbcChSession, Throwable, Unit] = for {
    ch <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    _  <- ch.truncateTable(meta)
  } yield ()*/

  /*  private def insertFromQuery(
                               meta: ViewQueryMeta,
                               query: Query,
                               ch: chSess
                             ): ZIO[jdbcChSession, Throwable, Unit] = for {
    ch <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    _  <- ch.insertFromQuery(meta, query.params)
  } yield ()*/

  /*  private def truncTable(meta: ViewQueryMeta): ZIO[jdbcChSession, Throwable, Unit] = for {
    ch <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    _  <- ch.truncateTable(meta)
  } yield ()

  private def insertFromQuery(
    meta: ViewQueryMeta,
    query: Query
  ): ZIO[jdbcChSession, Throwable, Unit] = for {
    ch <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    _  <- ch.insertFromQuery(meta, query.params)
  } yield ()*/

  /*  private def calcQuery(
    meta: ViewQueryMeta,
    query: Query
  ): ZIO[jdbcChSession, Throwable, Unit] = for {
    _ <- ZIO.logInfo(
           s"calcQuery for query_id = ${query.query_id} clickhouse table = ${meta.chTable}"
         )
    _ <- truncTable(meta)
    _ <- insertFromQuery(meta, query)
  } yield ()*/

  /**
   *   1. get meta data about this calculation from oracle database. 2.
   */
  def startCalculation(
    query: Query,
    // servers: Servers,
    meta: ViewQueryMeta,
    ora: oraSessCalc,
    id_reload_calc: Int
  ): ZIO[ImplCalcRepo with jdbcChSession, Throwable, Int] = for {
    _          <- ZIO.logDebug("startCalculation")
    repo      <- ZIO.service[ImplCalcRepo]
    _          <- ZIO.logInfo(s"ch_table = ${meta.chTable} params_count = ${meta.params.size}")
    queryLogId <- ora.insertViewQueryLog(query, id_reload_calc)
    ch         <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    calcQuery   = ch.truncateTable(meta) *>
                    ch.insertFromQuery(meta, query.params)
    _          <- calcQuery.tapError(er =>
                    ZIO.logError(s" startCalculation - calcEffect ${er.getMessage}") *>
                      ora.saveCalcError(queryLogId, er.getMessage) *>
                      repo.clearCalc
                  )
    _          <- ora.saveEndCalculation(queryLogId)
  } yield queryLogId

  def copyDataChOra(
    query: Query,
    meta: ViewQueryMeta,
    ora: oraSessCalc,
    queryLogId: Int
  ): ZIO[ImplCalcRepo with jdbcChSession, Throwable, Unit] = for {
    _         <- ZIO.logInfo(s"Begin copyDataChOra for query_id = ${query.query_id} ${meta.chTable}")
    ch        <- ZIO.serviceWithZIO[jdbcChSession](_.sess(0))
    repo      <- ZIO.service[ImplCalcRepo]
    chTableRs <- ch.getChTableResultSet(meta)
    _         <- ora.saveBeginCopying(queryLogId)
    _         <- ora.truncateTable(meta.oraSchema, meta.oraTable)
    _         <- ora
                   .insertRsDataInTable(chTableRs, meta.oraTable, meta.oraSchema)
                   .tapError(er => ZIO.logError(s"insertRsDataInTable - ${er.getMessage}") *>
                     ora.saveCalcError(queryLogId, er.getMessage) *>
                     repo.clearCalc
                   )
    _         <- ora.saveEndCopying(queryLogId)
    _         <-
      ZIO.logInfo(
        s"End copyDataChOra for query_id = ${query.query_id}"
      )
  } yield ()

}
