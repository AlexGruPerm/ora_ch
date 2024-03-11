package server

import calc.CalcLogic.getCalcMeta
import calc.{ CalcLogic, ImplCalcRepo, ReqCalc, ReqCalcSrc }
import clickhouse.{ chSess, jdbcChSession, jdbcChSessionImpl }
import common.Types.MaxValAndCnt
import conf.OraServer
import error.ResponseMessage
import ora.{ jdbcSession, jdbcSessionImpl, oraSessTask }
import request.{ Parallel, ReqNewTask, SrcTable }
import task.{ ImplTaskRepo, WsTask }
import zio._
import zio.http._
import zio.json.{ DecoderOps, EncoderOps, JsonDecoder }
import request.EncDecReqNewTaskImplicits._
import calc.EncDecReqCalcImplicits._
import common.{ SessCalc, SessTask, SessTypeEnum, TaskState, Wait, _ }
import connrepo.OraConnRepoImpl
import server.WServer.{ calc, startTask }
import table.Table
import zio.json.JsonDecoder.fromCodec

import java.io.IOException
import java.sql.SQLException
import scala.collection.immutable.List

object WServer {

  private def updatedCopiedRowsCount(
    table: Table,
    ora: oraSessTask,
    ch: chSess,
    maxValCnt: Option[MaxValAndCnt]
  ): ZIO[Any, Exception, Long] = for {
    copiedRows <- ch.getCountCopiedRows(table)
    rowCount   <-
      ora.updateCountCopiedRows(table, copiedRows - maxValCnt.map(_.CntRows).getOrElse(0L))
  } yield rowCount

  private def updatedCopiedRowsCountFUpd(
    srcTable: Table,
    targetTable: Table,
    ora: oraSessTask,
    ch: chSess
  ): ZIO[Any, Nothing, Long] = for {
    copiedRows <- ch.getCountCopiedRowsFUpd(targetTable)
    rowCount   <- ora.updateCountCopiedRows(srcTable, copiedRows)
  } yield rowCount

  private def saveError(
    sess: oraSessTask,
    errorMsg: String,
    updateFiber: Fiber.Runtime[Exception, Long],
    table: Table
  ): ZIO[ImplTaskRepo, Throwable, Unit] =
    for {
      _    <- sess.setTaskError(errorMsg, table)
      _    <- updateFiber.interrupt
      repo <- ZIO.service[ImplTaskRepo]
      _    <- ZIO.logError(s"Error for table [${table.fullTableName()}] - $errorMsg")
      _    <- repo.setState(TaskState(Wait))
      _    <- repo.clearTask
    } yield ()

  private def copyTableEffect(
    taskId: Int,
    sess: oraSessTask,
    sessCh: chSess,
    table: Table,
    fetch_size: Int,
    batch_size: Int
  ): ZIO[ImplTaskRepo, Throwable, Long] = for {
    _            <- ZIO.logInfo(s"Begin copyTableEffect for ${table.name}")
    _            <- sess.setTableBeginCopy(table)
    maxValAndCnt <- sessCh.getMaxColForSync(table)
    _            <- ZIO.logDebug(s"maxValAndCnt = $maxValAndCnt")
    arity         = table.syncArity()
    _            <- ZIO.logDebug(s"arity = $arity")
    appendKeys   <- arity match {
                      case 1 => sessCh.whereAppendInt1(table)
                      case 2 => sessCh.whereAppendInt2(table)
                      case 3 => sessCh.whereAppendInt3(table)
                      case _ => ZIO.succeed(None)
                    }
    _            <- ZIO.logDebug(s"appendKeys = $appendKeys")
    fiberUpdCnt  <- updatedCopiedRowsCount(table, sess, sessCh, maxValAndCnt)
                      .delay(2.second)
                      .repeat(Schedule.spaced(5.second))
                      .fork
    _            <-
      ZIO.logDebug(
        s"syncColMax = ${table.sync_by_column_max} syncUpdateColMax = ${table.sync_update_by_column_max}"
      )
    rs            = sess.getDataResultSet(taskId, table, fetch_size, maxValAndCnt, appendKeys)
    rowCount     <- sessCh
                      .recreateTableCopyData(table, rs, batch_size, maxValAndCnt)
                      .tapError(er =>
                        ZIO.logError(er.getMessage) *>
                          // fiber will be interrupted inside
                          saveError(sess, er.getMessage, fiberUpdCnt, table)
                      )
                      .refineToOrDie[SQLException]
    _            <- fiberUpdCnt.interrupt
    _            <- sess.setTableCopied(table, rowCount, table.finishStatus())
    _            <-
      ZIO.logInfo(
        s"taskId=[$taskId] copyTableEffect Finished for table = ${table.name} with rowsCount = $rowCount"
      )
  } yield rowCount

  private def updateTableColumns(
    sess: oraSessTask,
    sessCh: chSess,
    table: Table,
    fetch_size: Int,
    batch_size: Int
  ): ZIO[ImplTaskRepo, Throwable, Long] =
    sess.setTableBeginCopy(table) *>
      sessCh.getPkColumns(table).flatMap { pkColList =>
        updatedCopiedRowsCountFUpd(table, table.copy(name = s"upd_${table.name}"), sess, sessCh)
          .repeat(Schedule.spaced(5.second))
          .fork *>
          sessCh
            .recreateTableCopyDataForUpdate(
              table.copy(name = s"upd_${table.name}"),
              sess.getDataResultSetForUpdate(table, fetch_size, pkColList),
              batch_size,
              pkColList
            )
            .flatMap { rowCount =>
              sess.setTableCopied(table, rowCount, table.finishStatus()) *>
                sessCh.updateColumns(
                  table,
                  table.copy(name = s"upd_${table.name}"),
                  pkColList
                ) *> ZIO.succeed(rowCount)
            }
      }

  private def closeSession(s: oraSessTask, table: Table): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           s.sess.commit()
           s.sess.close()
         }.refineToOrDie[SQLException]
           .tapError(er =>
             ZIO.logError(s"closeSession [${table.fullTableName()}] error: ${er.getMessage}")
           )
  } yield ()

  private def startTask(
    newtask: ReqNewTask
  ): ZIO[ImplTaskRepo with jdbcChSession with jdbcSession, Throwable, Unit] = for {
    _          <- ZIO.logDebug(s" newtask = $newtask")
    oraSess    <- ZIO.service[jdbcSession]
    repo       <- ZIO.service[ImplTaskRepo]
    jdbcCh     <- ZIO.service[jdbcChSession]
    sess       <- oraSess.sessTask()
    taskId     <- sess.getTaskIdFromSess
    _          <- repo.setTaskId(taskId)
    t          <- sess.getTables(newtask.schemas)
    wstask      = WsTask(
                    id = taskId,
                    state = TaskState(Ready, Option.empty[Table]),
                    oraServer = Some(newtask.servers.oracle),
                    clickhouseServer = Some(newtask.servers.clickhouse),
                    parallel = newtask.parallel,
                    tables = t,
                    newtask.parallel.degree
                  )
    _          <- repo.create(wstask)
    _          <- repo.setState(TaskState(Executing))
    taskId     <- repo.getTaskId
    sessCh     <- jdbcCh.sess(taskId)
    task       <- repo.ref.get
    setSchemas  = task.tables.map(_.schema).toSet diff Set("system", "default", "information_schema")
    _          <- sess.saveTableList(task.tables).tapSomeError { case er: SQLException =>
                    ZIO.logError(s"saveTableList ${er.getMessage}") *> repo.setState(TaskState(Wait))
                  }
    _          <- sessCh.createDatabases(setSchemas)
    _          <- sess.setTaskState("executing")
    fetch_size  = task.oraServer.map(_.fetch_size).getOrElse(1000)
    batch_size  = task.clickhouseServer.map(_.batch_size).getOrElse(1000)
    copyEffects = task.tables.map { table =>
                    oraSess.sessTask(Some(taskId)).flatMap { s =>
                      (
                        if (table.sync_update_by_column_max.nonEmpty)
                          copyTableEffect(task.id, s, sessCh, table, fetch_size, batch_size) *>
                            updateTableColumns(s, sessCh, table, fetch_size, batch_size)
                        else if (table.update_fields.nonEmpty)
                          updateTableColumns(s, sessCh, table, fetch_size, batch_size)
                        else
                          copyTableEffect(task.id, s, sessCh, table, fetch_size, batch_size)
                      )
                        .refineToOrDie[SQLException] *>
                        closeSession(s, table)
                    }
                  }
    _          <- if (wstask.parDegree <= 2)
                    ZIO.logInfo(s"copyEffects sequentially with parDegree = ${wstask.parDegree}") *>
                      ZIO.collectAll(copyEffects)
                  else
                    ZIO.logInfo(s"copyEffects IN PARALLEL with parDegree = ${wstask.parDegree}") *>
                      ZIO.collectAllPar(copyEffects).withParallelism(wstask.parDegree - 1)

    _ <- repo.setState(TaskState(Wait))
    _ <- repo.clearTask
    _ <- sess.taskFinished
  } yield ()

  private def requestToEntity[A](
    r: Request
  )(implicit decoder: JsonDecoder[A]): ZIO[Any, Nothing, Either[String, A]] = for {
    req <- r.body
             .asString
             .map(_.fromJson[A])
             .catchAllDefect { case e: Exception =>
               ZIO.logError(s"Error[3] parsing input file with : ${e.getMessage}") *>
                 ZIO.succeed(Left(e.getMessage))
             }
             .catchAll { case e: Exception =>
               ZIO.logError(s"Error[4] parsing input file with : ${e.getMessage}") *>
                 ZIO.succeed(Left(e.getMessage))
             }
  } yield req

  private def currStatusCheckerTask(): ZIO[ImplTaskRepo, Throwable, Unit] =
    for {
      repo       <- ZIO.service[ImplTaskRepo]
      currStatus <- repo.getState
      taskId     <- repo.getTaskId
      _          <- ZIO.logDebug(s"Repo currStatus = ${currStatus.state}")
      _          <-
        ZIO
          .fail(
            new Exception(
              s" already running id = $taskId ,look at tables: ora_to_ch_tasks, ora_to_ch_tasks_tables "
            )
          )
          .when(currStatus.state != Wait)
    } yield ()

  private def currStatusCheckerCalc(): ZIO[ImplCalcRepo, Throwable, Unit] =
    for {
      repo       <- ZIO.service[ImplCalcRepo]
      currStatus <- repo.getState
      taskId     <- repo.getCalcId
      _          <- ZIO.logInfo(s"Repo currStatus = ${currStatus.state}")
      _          <- ZIO
                      .fail(
                        new Exception(
                          s" already running id = $taskId ,look at tables: ora_to_ch_views_query_log"
                        )
                      )
                      .when(currStatus.state != Wait)
    } yield ()

  /**
   * When task is started as startTask(newTask).provide(...).forkDaemon we want save error in oracle
   * db when fiber fail or die. This catcher take this error and save it into db.
   */
  private def errorCatcherForkedTask(
    taskEffect: ZIO[Any, Throwable, Unit]
    // taskFiber: Fiber.Runtime[Throwable, Unit]
  ): ZIO[Any, Throwable, Unit] = for {
    fn        <- ZIO.fiberId.map(_.threadName)
    _         <- ZIO.logDebug(s"Error catcher started on $fn")
    /**
     * await is similar to join, but they react differently to errors and interruption: await always
     * succeeds with Exit information, even if the fiber fails or is interrupted. In contrast to
     * that, join on a fiber that fails will itself fail with the same error as the fiber
     */
    taskFiber <- taskEffect.fork
    exitValue <- taskFiber.await
    _         <- exitValue match {
                   case Exit.Success(value) =>
                     ZIO.logInfo(s"startTask completed successfully.")
                   case Exit.Failure(cause) =>
                     ZIO.logError(s"**** Fiber failed ****") *>
                       ZIO.logError(s"${cause.prettyPrint}")
                 }
    _         <- taskFiber.interrupt
  } yield ()

  private def task(
    req: Request,
    waitSeconds: Int
  ): ZIO[ImplTaskRepo with SessTypeEnum, Throwable, Response] = for {
    u        <- requestToEntity[ReqNewTask](req)
    response <- u match {
                  case Left(errorString) => ZioResponseMsgBadRequest(errorString)
                  case Right(newTask)    =>
                    for {
                      repo            <- ZIO.service[ImplTaskRepo]
                      _               <- currStatusCheckerTask()
                      layerOraConnRepo =
                        OraConnRepoImpl.layer(newTask.servers.oracle, newTask.parallel, "Ucp_task")
                      taskEffect       = startTask(newTask)
                                           .provide(
                                             layerOraConnRepo,
                                             ZLayer.succeed(repo),
                                             ZLayer.succeed(
                                               newTask.servers.oracle
                                             ) >>> jdbcSessionImpl.layer,
                                             ZLayer.succeed(
                                               newTask.servers.clickhouse
                                             ) >>> jdbcChSessionImpl.layer,
                                             ZLayer.succeed(SessCalc)
                                           )
                      _               <- errorCatcherForkedTask(taskEffect).forkDaemon
                      schedule         = Schedule.spaced(250.millisecond) && Schedule.recurs(waitSeconds)
                      taskId          <-
                        repo
                          .getTaskId
                          .filterOrFail(_ != 0)(0.toString)
                          .retryOrElse(
                            schedule,
                            (_: String, _: (Long, Long)) =>
                              ZIO.fail(
                                new Exception(
                                  s"Elapsed wait time $waitSeconds seconds of getting taskId"
                                )
                              )
                          )
                    } yield Response.json(s"""{"taskid": "$taskId"}""").status(Status.Ok)
                }
  } yield response

  private def calcAndCopy(
    reqCalc: ReqCalcSrc,
    waitSeconds: Int
  ): ZIO[ImplCalcRepo with SessTypeEnum, Throwable, Response] = for {
    repo   <- ZIO.service[ImplCalcRepo]
    _      <- currStatusCheckerCalc()
    _      <- (
                CalcLogic
                  .getOraConnFromPool()
                  .flatMap { ora =>
                    CalcLogic.getCalcMeta(ora, reqCalc).flatMap { meta =>
                      CalcLogic.startCalculation(ora, reqCalc, meta) *>
                        CalcLogic.copyDataChOra(ora, reqCalc, meta)
                    }
                  }
                )
                .provide(
                  OraConnRepoImpl.layer(reqCalc.servers.oracle, Parallel(), "Ucp_calc"),
                  ZLayer.succeed(repo),
                  ZLayer.succeed(reqCalc.servers.oracle) >>> jdbcSessionImpl.layer,
                  ZLayer.succeed(reqCalc.servers.clickhouse) >>> jdbcChSessionImpl.layer,
                  ZLayer.succeed(SessCalc)
                )
                .forkDaemon
    sched   = Schedule.spaced(1.second) && Schedule.recurs(waitSeconds)
    calcId <- repo
                .getCalcId
                .filterOrFail(_ != 0)(0.toString)
                .retryOrElse(
                  sched,
                  (_: String, _: (Long, Long)) =>
                    ZIO.fail(new Exception("Elapsed wait time 10 seconds of getting calcId"))
                )
  } yield Response.json(s"""{"calcId":"$calcId"}""").status(Status.Ok)

  private def calc(
    req: Request,
    waitSeconds: Int
  ): ZIO[ImplCalcRepo with SessTypeEnum, Throwable, Response] = for {
    bodyText <- req.body.asString
    _        <- ZIO.logDebug(s"calc body = $bodyText")
    reqCalcE <- requestToEntity[ReqCalcSrc](req)
    _        <- ZIO.logDebug(s"JSON = $reqCalcE")
    resp     <- reqCalcE match {
                  case Left(exp_str) => ZioResponseMsgBadRequest(exp_str)
                  case Right(src)    => calcAndCopy(src, waitSeconds)
                }
  } yield resp

  private def getMainPage: ZIO[Any, IOException, Response] =
    ZIO.fail(new IOException("error text in IOException"))

  private def ZioResponseMsgBadRequest(message: String): ZIO[Any, Nothing, Response] =
    ZIO.succeed(Response.json(ResponseMessage(message).toJson).status(Status.InternalServerError))

  /**
   * Add catchAll common part to effect.
   */
  private def catchCover[C](eff: ZIO[C, Throwable, Response]): ZIO[C, Nothing, Response] =
    eff.catchAll { e: Throwable =>
      ZIO.logError(e.getMessage) *> ZioResponseMsgBadRequest(e.getMessage)
    }

  private val routes: Int => Routes[ImplTaskRepo with ImplCalcRepo, Nothing] = waitSeconds =>
    Routes(
      Method.POST / "task"  -> handler { (req: Request) =>
        catchCover(task(req, waitSeconds).provideSome[ImplTaskRepo](ZLayer.succeed(SessTask)))
      },
      Method.POST / "calc"  -> handler { (req: Request) =>
        catchCover(calc(req, waitSeconds).provideSome[ImplCalcRepo](ZLayer.succeed(SessCalc)))
      },
      Method.GET / "random" -> handler(Random.nextString(10).map(Response.text(_))),
      Method.GET / "utc"    -> handler(Clock.currentDateTime.map(s => Response.text(s.toString))),
      Method.GET / "main"   -> handler(catchCover(getMainPage)),
      Method.GET / "text"   -> handler(ZIO.succeed(Response.text("Hello World 2!"))),
      Method.GET / "json"   -> handler(
        ZIO.succeed(Response.json("""{"greetings": "Hello World! 2"}"""))
      )
    )

  val app: Int => HttpApp[ImplTaskRepo with ImplCalcRepo] = waitSeconds =>
    routes(waitSeconds).toHttpApp
}
