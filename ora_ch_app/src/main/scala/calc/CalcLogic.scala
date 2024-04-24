package calc

import calc.QueryParDivider.{listOfListsQuery, mapOfQueues}
import clickhouse.{chSess, jdbcChSession, jdbcChSessionImpl}
import common.{CalcState, Executing, SessCalc, SessTypeEnum, Wait}
import connrepo.OraConnRepoImpl
import ora.{jdbcSession, jdbcSessionImpl, oraSessCalc}
import zio.{ZIO, ZLayer}
import zio.http.{Response, Status}

/**
 * Contains functions that related with process of calculation in clickhouse.
 */
object CalcLogic {

  private def getCalcMeta(query: Query, ora: oraSessCalc): ZIO[Any, Throwable, ViewQueryMeta] = for {
    _    <- ZIO.logInfo(s"getCalcMeta for query_id = ${query.query_id}")
    meta <- ora.getQueryMeta(query.query_id)
  } yield meta

  /**
   *   1. get meta data about this calculation from oracle database. 2.
   */
  private def startCalculation(
    query: Query,
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

  private def copyDataChOra(
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
    _          <- ora.saveBeginCopying(queryLogId)
    _          <- ora.truncateTable(meta.oraSchema, meta.oraTable)
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

  private def getCalcAndCopyEffect(
                                    oraSess: jdbcSession,
                                    q: Query,
                                    idReloadCalcId: Int
                                  ): ZIO[ImplCalcRepo with jdbcChSession with jdbcSession, Throwable, Unit] = for {
    s          <- oraSess.sessCalc(debugMsg = "calc - copyDataChOra")
    meta       <- CalcLogic.getCalcMeta(q, s)
    queryLogId <- CalcLogic.startCalculation(q, meta, s, idReloadCalcId)
    eff        <- CalcLogic.copyDataChOra(q, meta, s, queryLogId)
  } yield eff

  private def executeCalcAndCopy(
                                  reqCalc: ReqCalcSrc
                                ): ZIO[ImplCalcRepo with jdbcChSession with jdbcSession, Throwable, Unit] = for {
    _         <-
      ZIO.logInfo(s"executeCalcAndCopy for reqCalc = ${reqCalc.queries.map(_.query_id).toString()}")
    oraSess   <- ZIO.service[jdbcSession]
    queries   <- ZIO.succeed(reqCalc.queries)
    repo      <- ZIO.service[ImplCalcRepo]
    repoState <- repo.getState
    _         <- ZIO.logInfo(s"repo state = $repoState")

    /**
     * TODO: old algo of simple seq executions. calcsEffects = queries.map(query =>
     * oraSess.sessCalc(debugMsg = "calc - copyDataChOra").flatMap { s =>
     * CalcLogic.getCalcMeta(query, s).flatMap { meta => CalcLogic.startCalculation(query, meta, s,
     * reqCalc.id_reload_calc).flatMap { queryLogId => CalcLogic.copyDataChOra(query, meta, s,
     * queryLogId) } } } ) _ <- ZIO.collectAll(calcsEffects)
     */

    calcsEffects =
      listOfListsQuery(mapOfQueues(queries)).map(query =>
        query.map(q => (q.query_id, getCalcAndCopyEffect(oraSess, q, reqCalc.id_reload_calc)))
      )

    _ <- ZIO.foreachDiscard(calcsEffects) { lst =>
      ZIO.logInfo(s"parallel execution of ${lst.map(_._1)} ") *>
        ZIO
          .collectAllPar(lst.map(_._2))
          .withParallelism(2 /*SEQ-PAR*/ )
          .tapError(_ => repo.clearCalc)
    }

    _ <- repo.clearCalc
  } yield ()

  private def currStatusCheckerCalc(): ZIO[ImplCalcRepo, Throwable, Unit] =
    for {
      repo       <- ZIO.service[ImplCalcRepo]
      currStatus <- repo.getState
      _          <- ZIO.logInfo(s"Repo currStatus = ${currStatus.state}")
      _          <- ZIO
        .fail(
          new Exception(
            s" already running, look at tables: ora_to_ch_query_log"
          )
        )
        .when(currStatus.state != Wait)
    } yield ()

   def calcAndCopy(reqCalc: ReqCalcSrc): ZIO[ImplCalcRepo with SessTypeEnum, Throwable, Response] = for {
    repo <- ZIO.service[ImplCalcRepo]
    _    <- currStatusCheckerCalc()
    _    <- repo.create(ReqCalc(id = reqCalc.id_reload_calc))
    _    <- repo.setState(CalcState(Executing))
    /**
     * We can use parallel execution only with degree =2. Because listOfListsQuery(mapOfQueues
     * algorithm make List of lists Query, List.size=2 Parallel(degree = 2)
     */
    _    <- executeCalcAndCopy(reqCalc)
      .provide(
        OraConnRepoImpl.layer(reqCalc.servers.oracle, 2),
        ZLayer.succeed(repo),
        ZLayer.succeed(reqCalc.servers.oracle) >>> jdbcSessionImpl.layer,
        ZLayer.succeed(reqCalc.servers.clickhouse) >>> jdbcChSessionImpl.layer,
        ZLayer.succeed(SessCalc)
      )
      .forkDaemon
  } yield Response.json(s"""{"calcId":"ok"}""").status(Status.Ok)

}
