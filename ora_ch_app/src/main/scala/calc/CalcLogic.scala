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
    repo       <- ZIO.service[ImplCalcRepo]
    _          <- ZIO.logInfo(s"ch_table = ${meta.chTable} params_count = ${meta.params.size}")
    queryLogId <- ora.insertViewQueryLog(query, id_reload_calc)
    // -------------------------
    clickhouse <- ZIO.service[jdbcChSession]
    chPool     <- clickhouse.getClickHousePool()
    ch         <- ZIO.succeed(chSess(chPool.getConnection, 0))
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
    _ <- ZIO.logInfo(s"Begin copyDataChOra for query_id = ${query.query_id} ${meta.chTable}")

    clickhouse <- ZIO.service[jdbcChSession]
    chPool     <- clickhouse.getClickHousePool()
    ch         <- ZIO.succeed(chSess(chPool.getConnection, 0))
    repo       <- ZIO.service[ImplCalcRepo]
    // chTableRs  <- ch.getChTableResultSet(meta)
    _          <- ora.saveBeginCopying(queryLogId)
    _          <- ora.truncateTable(meta.oraSchema, meta.oraTable)
    /*
    _         <- ora.insertRsDataInTable(chTableRs, meta.oraTable, meta.oraSchema)
                   .tapError(er =>
                     ZIO.logError(s"insertRsDataInTable - ${er.getMessage}") *>
                       ora.saveCalcError(queryLogId, er.getMessage) *>
                       repo.clearCalc
                   )*/
    _          <- ch.copyTableChOra(meta)
                    .tapError(er =>
                      ZIO.logError(s"copyDataChOra - ${er.getMessage}") *>
                        ora.saveCalcError(queryLogId, er.getMessage) *>
                        repo.clearCalc
                    )
    _          <- ora.saveEndCopying(queryLogId, meta)
    _          <-
      ZIO.logInfo(
        s"End copyDataChOra for query_id = ${query.query_id}"
      )
  } yield ()

}
