package calc

import calc.QueryParDivider.{ listOfListsQuery, mapOfQueues }
import clickhouse.{ chSess, jdbcChSession, jdbcChSessionImpl }
import common.{ CalcState, Executing, SessCalc, SessTypeEnum, Wait }
import connrepo.OraConnRepoImpl
import ora.{ jdbcSession, jdbcSessionImpl, oraSessCalc, oraSessTask }
import table.Table
import zio.{ durationInt, ZIO, ZLayer }
import zio.http.{ Response, Status }

import java.sql.{ Connection, SQLException }

/**
 * Contains functions that related with process of calculation in clickhouse.
 */
object CalcLogic {

  private def getCalcMeta(query: Query, ora: oraSessCalc): ZIO[Any, SQLException, ViewQueryMeta] =
    ora.getQueryMeta(query.query_id)

  private def startCalculation(
    query: Query,
    meta: ViewQueryMeta,
    ora: oraSessCalc,
    queryLogId: Int
  ): ZIO[ImplCalcRepo with jdbcChSession, Throwable, Unit] = for {
    _          <- ZIO.logInfo(s"Start Calculation QL.ID:$queryLogId SID:${ora.getPid} [${meta.chTable}]")
    repo       <- ZIO.service[ImplCalcRepo]
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
  } yield ()

  private def copyDataChOra(
    query: Query,
    meta: ViewQueryMeta,
    ora: oraSessCalc,
    queryLogId: Int
  ): ZIO[ImplCalcRepo with jdbcChSession, Throwable, Unit] = for {
    fn         <- ZIO.fiberId.map(_.threadName)
    _          <- ZIO.logInfo(s"fiber:$fn copyDataChOra SID = [${ora.getPid}] query_id = ${query.query_id} ")
    clickhouse <- ZIO.service[jdbcChSession]
    chPool     <- clickhouse.getClickHousePool()
    ch         <- ZIO.succeed(chSess(chPool.getConnection, 0))
    repo       <- ZIO.service[ImplCalcRepo]
    _          <- ora.saveBeginCopying(queryLogId)
    _          <- ora.truncateTable(meta.oraSchema, meta.oraTable)
    // _          <- ch.optimizeTable(meta).when(meta.chTable == "ch_cache_for_calc_12904_11487")
    _          <- ch.copyTableChOra(meta)
                    .tapError(er =>
                      ZIO.logError(s"copyDataChOra - ${er.getMessage}") *>
                        ora.saveCalcError(queryLogId, er.getMessage) *> //todo: remove it, handled outside and save in ora log.
                        repo.clearCalc
                    )
    _          <- ora.saveEndCopying(queryLogId, meta)
    _          <-
      ZIO.logInfo(
        s"End copyDataChOra for query_id = ${query.query_id}"
      )
  } yield ()

  private def closeSession(con: oraSessCalc): ZIO[Any, SQLException, Unit] =
    for {
      _ <- ZIO.logInfo(s">>>>>>>>>>>>>>>> closeSession for ${con.calcId} >>>>>>>>>>>>>>")
      _ <- ZIO.attemptBlockingInterrupt {
             con.sess.commit()
             con.sess.close()
           }.refineToOrDie[SQLException]
             .tapError(er => ZIO.logError(s"closeSession error: ${er.getMessage}"))
    } yield ()

  private def copyLocalCache(
    meta: ViewQueryMeta,
    ora: oraSessCalc,
    queryLogId: Int
  ): ZIO[ImplCalcRepo with jdbcChSession, Throwable, Unit] = for {
    fn <- ZIO.fiberId.map(_.threadName)
    _  <- ZIO.logInfo(
        s"fiber:$fn copyLocalCache (${meta.chSchema}) from ${meta.chTable} to ${meta.chTable.replace("ch_", "")}"
      )
    clickhouse <- ZIO.service[jdbcChSession]
    chPool     <- clickhouse.getClickHousePool() // todo: may be shorter .map(ds => chSess(ds.getConnection, 0))
    ch         <- ZIO.succeed(chSess(chPool.getConnection, 0))

    _  <- ora.saveBeginLocalCopying(queryLogId)
    _  <- ZIO.logInfo(" >>>>>>>>>>>>>>>> LOCAL COPYING BEGIN <<<<<<<<<<<<<<<<")
    // _ <- ZIO.fail(new RuntimeException("LOCAL COPYING Error")) todo: remove Это обрабатывает вызовом снаружи и пишется в ора лог.
    _  <- ch.copyInLocalCache(meta)
    _  <- ZIO.logInfo(" >>>>>>>>>>>>>>>> LOCAL COPYING BEGIN <<<<<<<<<<<<<<<<")
    _  <- ora.saveEndLocalCopying(queryLogId)
    _  <- ZIO.logInfo(s">>> DEBUG = ${ora.getPid} queryLogId = $queryLogId")
  } yield ()

  private def getCalcAndCopyEffect(
    ora: oraSessCalc,
    q: Query,
    queryLogId: Int
  ): ZIO[ImplCalcRepo with jdbcChSession with jdbcSession, Throwable, Unit] = for {
    meta <- CalcLogic.getCalcMeta(q, ora)
    _    <- CalcLogic.startCalculation(q, meta, ora, queryLogId)
    // todo: try parallel execution of and copyDataChOra
    _    <- CalcLogic.copyLocalCache(meta, ora, queryLogId).when(q.copy_to_local_cache == 1)
    _    <- CalcLogic.copyDataChOra(q, meta, ora, queryLogId)
  } yield ()

  private def executeCalcAndCopy(
    reqCalc: ReqCalcSrc
  ): ZIO[ImplCalcRepo with jdbcChSession with jdbcSession, Throwable, Unit] = for {
    _           <-
      ZIO.logInfo(
        s"executeCalcAndCopy for reqCalc = ${reqCalc.queries.sortBy(_.order_by).map(_.query_id).toString()}"
      )
    oraSess     <- ZIO.service[jdbcSession]
    queries     <- ZIO.succeed(reqCalc.queries.sortBy(_.order_by))
    repo        <- ZIO.service[ImplCalcRepo]
    repoState   <- repo.getState
    _           <- ZIO.logInfo(s"repo state = $repoState")
    calcsEffects =
      queries.map(q =>
        oraSess
          .sessCalc(debugMsg = s"calc [${q.query_id}] ord: [${q.order_by}] - executeCalcAndCopy")
          .flatMap { ora =>
            ora
              .insertViewQueryLog(q, reqCalc.id_reload_calc)
              .flatMap(queryLogId =>
                getCalcAndCopyEffect(ora, q, queryLogId)
                  .tapError(er =>
                    ZIO.logError(s"getCalcAndCopyEffect error - ${er.getMessage}") *>
                      ora.saveCalcError(queryLogId, er.getMessage) *>
                      repo.clearCalc *>
                      closeSession(ora)
                  )
              )
          }
      )
    _           <- ZIO.foreachDiscard(calcsEffects) { r =>
                     r.tapError(_ => repo.clearCalc)
                   }
    _           <- repo.clearCalc
    repoState   <- repo.getState
    _           <- ZIO.logInfo(s"Finish repo state = $repoState")
    calcId      <- repo.getCalcId
    _           <- ZIO.logInfo(s"Current calcId = $calcId")
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

  def calcAndCopy(reqCalc: ReqCalcSrc): ZIO[ImplCalcRepo with SessTypeEnum, Throwable, Response] =
    for {
      repo <- ZIO.service[ImplCalcRepo]
      _    <- currStatusCheckerCalc()
      _    <- repo.create(ReqCalc(id = reqCalc.id_reload_calc))
      _    <- repo.setState(CalcState(Executing))
      _    <- executeCalcAndCopy(reqCalc)
                .provide(
                  OraConnRepoImpl.layer(reqCalc.servers.oracle, 1),
                  ZLayer.succeed(repo),
                  ZLayer.succeed(reqCalc.servers.oracle) >>> jdbcSessionImpl.layer,
                  ZLayer.succeed(reqCalc.servers.clickhouse) >>> jdbcChSessionImpl.layer,
                  ZLayer.succeed(SessCalc)
                )
                .forkDaemon
    } yield Response.json(s"""{"calcId":"ok"}""").status(Status.Ok)

}
