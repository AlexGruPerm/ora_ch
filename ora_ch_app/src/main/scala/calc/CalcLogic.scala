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
    _          <- ZIO.logInfo(s"copyDataChOra SID = [${ora.getPid}] query_id = ${query.query_id} ")
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

  private def closeSession(con: oraSessCalc): ZIO[Any, SQLException, Unit] =
    for {
      _    <- ZIO.logInfo(s">>>>>>>>>>>>>>>> closeSession for ${con.calcId} >>>>>>>>>>>>>>")
      _    <- ZIO.attemptBlockingInterrupt {
                con.sess.commit()
                con.sess.close()
              }.refineToOrDie[SQLException]
                .tapError(er => ZIO.logError(s"closeSession error: ${er.getMessage}"))
    } yield ()

  private def getCalcAndCopyEffect(
    ora: oraSessCalc,
    q: Query,
    queryLogId: Int
  ): ZIO[ImplCalcRepo with jdbcChSession with jdbcSession, Throwable, Unit] = for {
    meta       <- CalcLogic.getCalcMeta(q, ora)
    _          <- CalcLogic.startCalculation(q, meta, ora, queryLogId)
    eff        <- CalcLogic.copyDataChOra(q, meta, ora, queryLogId)
  } yield eff

  private def executeCalcAndCopy(
    reqCalc: ReqCalcSrc
  ): ZIO[ImplCalcRepo with jdbcChSession with jdbcSession, Throwable, Unit] = for {
    _           <-
      ZIO.logInfo(s"executeCalcAndCopy for reqCalc = ${reqCalc.queries.map(_.query_id).toString()}")
    oraSess     <- ZIO.service[jdbcSession]
    queries     <- ZIO.succeed(reqCalc.queries)
    repo        <- ZIO.service[ImplCalcRepo]
    repoState   <- repo.getState
    _           <- ZIO.logInfo(s"repo state = $repoState")
    calcsEffects =
      listOfListsQuery(mapOfQueues(queries)).map(query =>
        query.map(q =>
          (
            q.query_id, //todo: rewrite in sep. func. with for-comprehension
            oraSess.sessCalc(debugMsg = "calc - executeCalcAndCopy").flatMap { ora =>
              ora.insertViewQueryLog(q, reqCalc.id_reload_calc)
                .flatMap(queryLogId => getCalcAndCopyEffect(ora, q, queryLogId)
                  .tapError(
                    er =>
                      ZIO.logError(s"getCalcAndCopyEffect error - ${er.getMessage}") *>
                        ora.saveCalcError(queryLogId, er.getMessage) *>
                        repo.clearCalc *>
                        closeSession(ora)
                  ))
            }
          )
        )
      )

    _ <- ZIO.foreachDiscard(calcsEffects) { lst =>
           ZIO.logInfo(s"parallel execution of ${lst.map(_._1)} ") *>
             ZIO
               .foreachPar(lst)(_._2)
               .withParallelism(2)
               .tapError(_ => repo.clearCalc)
         }
    _ <- repo.clearCalc
    repoState   <- repo.getState
    _           <- ZIO.logInfo(s"Finish repo state = $repoState")
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
