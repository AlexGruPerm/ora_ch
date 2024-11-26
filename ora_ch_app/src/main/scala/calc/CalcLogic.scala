package calc

import clickhouse.{chSess, jdbcChSession, jdbcChSessionImpl}
import common._
import connrepo.OraConnRepoImpl
import ora.{jdbcSession, jdbcSessionImpl, oraSessCalc}
import zio.ZIO.ifZIO
import zio.http.{Response, Status}
import zio.{ZIO, ZLayer}
import zio._

import java.sql.SQLException

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
    _        <- ZIO.logInfo(s"Start Calculation QL.ID:$queryLogId SID:${ora.getPid} [${meta.chTable}]")
    ch       <- ZIO
                  .serviceWithZIO[jdbcChSession](_.getClickHousePool())
                  .map(ds => chSess(ds.getConnection, 0))
    calcQuery = ch.truncateTable(meta) *>
                  ch.insertFromQuery(meta, query.params)
    _        <- calcQuery
    _        <- ora.saveEndCalculation(queryLogId)
  } yield ()

  private def copyDataChOra(
    query: Query,
    meta: ViewQueryMeta,
    ora: oraSessCalc,
    queryLogId: Int
  ): ZIO[ImplCalcRepo with jdbcChSession, Throwable, Unit] = for {
    fn <- ZIO.fiberId.map(_.threadName)
    _  <- ZIO.logInfo(s"fiber:$fn copyDataChOra SID = [${ora.getPid}] query_id = ${query.query_id} copy_by_parts_key=${query.copy_by_parts_key}")
    ch <- ZIO
            .serviceWithZIO[jdbcChSession](_.getClickHousePool())
            .map(ds => chSess(ds.getConnection, 0))
    _  <- ora.saveBeginCopying(queryLogId)
    _  <- ora.truncateTable(meta.oraSchema, meta.oraTable)
    condition = ZIO.succeed(query.copy_by_parts_key.nonEmpty)
    parCopy = ZIO.foreachParDiscard(1 to query.copy_by_parts_cnt.getOrElse(1))(partNum =>
      ch.copyTableChOraParts(meta, query.copy_by_parts_key.getOrElse("non_field"), query.copy_by_parts_cnt.getOrElse(1), partNum)
    ).when(query.copy_by_parts_key.nonEmpty)
    _ <- ifZIO(condition)(parCopy, ch.copyTableChOra(meta))
    _  <- ora.saveEndCopying(queryLogId, meta, query.copy_by_parts_cnt.getOrElse(1))
    _  <-
      ZIO.logInfo(
        s"End copyDataChOra for query_id = ${query.query_id}"
      )
  } yield ()

  private def closeSession(con: oraSessCalc): ZIO[Any, SQLException, Unit] =
    for {
      _ <- ZIO.logInfo(
             s">>>>>>>>>>>>>>>> closeSession sid=${con.getPid} for ${con.calcId} >>>>>>>>>>>>>>"
           )
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
    _  <- ZIO.logInfo(s"fiber:$fn copyLocalCache (${meta.chSchema})}")
    ch <- ZIO
            .serviceWithZIO[jdbcChSession](_.getClickHousePool())
            .map(ds => chSess(ds.getConnection, 0))
    _  <- ora.saveBeginLocalCopying(queryLogId)
    _  <- ch.copyInLocalCache(meta)
    _  <- ora.saveEndLocalCopying(queryLogId)
  } yield ()

  private def saveFinalFinished(
    ora: oraSessCalc,
    queryLogId: Int
  ): ZIO[ImplCalcRepo with jdbcChSession, Throwable, Unit] = for {
    fn <- ZIO.fiberId.map(_.threadName)
    _  <- ZIO.logInfo(s"fiber:$fn saveFinalFinished for logId=$queryLogId")
    _  <- ora.saveFinalFinished(queryLogId)
  } yield ()

  private def getCalcAndCopyEffect(
    ora: oraSessCalc,
    q: Query,
    queryLogId: Int
  ): ZIO[ImplCalcRepo with jdbcChSession with jdbcSession, Throwable, Unit] = for {
    meta    <- CalcLogic.getCalcMeta(q, ora)
    _       <- CalcLogic.startCalculation(q, meta, ora, queryLogId)
    locCopy <- CalcLogic.copyLocalCache(meta, ora, queryLogId).when(q.copy_to_local_cache == 1).fork
    oraCopy <- CalcLogic.copyDataChOra(q, meta, ora, queryLogId).fork
    _       <- locCopy.join
    _       <- oraCopy.join
    _       <- CalcLogic.saveFinalFinished(ora, queryLogId)
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
                  .tapBoth(
                    er =>
                      ZIO.logError(
                        s"getCalcAndCopyEffect error - ${er.getMessage} for $queryLogId"
                      ) *>
                        ora.saveCalcError(queryLogId, er.getMessage) *>
                        repo.clearCalc *>
                        closeSession(ora),
                    _ =>
                      ZIO.logInfo(s"Successful calculated for $queryLogId ") *>
                        ZIO.logInfo(s" oracle session ${ora.getPid} will be closed.") *>
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
