package server


import calc.CalcLogic.getCalcMeta
import calc.{CalcLogic, ImplCalcRepo, ReqCalc, ReqCalcSrc}
import clickhouse.{chSess, jdbcChSession, jdbcChSessionImpl}
import common.Types.MaxValAndCnt
import conf.OraServer
import error.ResponseMessage
import ora.{jdbcSession, jdbcSessionImpl, oraSessTask}
import request.{ReqNewTask, SrcTable}
import task.{ImplTaskRepo, WsTask}
import zio._
import zio.http._
import zio.json.{DecoderOps, EncoderOps, JsonDecoder}
import request.EncDecReqNewTaskImplicits._
import calc.EncDecReqCalcImplicits._
import common.{SessCalc, SessTask, SessTypeEnum, TaskState, Wait, _}
import connrepo.OraConnRepoImpl
import server.WServer.{calc, startTask}
import table.Table
import zio.json.JsonDecoder.fromCodec

import java.io.IOException
import scala.collection.immutable.List

object WServer {

  private def updatedCopiedRowsCount(table: Table, ora: oraSessTask, ch: chSess, maxValCnt: Option[MaxValAndCnt]):
  ZIO[Any,Nothing,Unit] = for {
    copiedRows <- ch.getCountCopiedRows(table)
    _ <- ora.updateCountCopiedRows(table,copiedRows - maxValCnt.map(_.CntRows).getOrElse(0L))
  } yield ()

  private def startTask(newtask: ReqNewTask): ZIO[ImplTaskRepo with jdbcChSession with jdbcSession, Throwable, Unit] = for {
    _ <- ZIO.logDebug(s"This is a debug message 2")
    repo <- ZIO.service[ImplTaskRepo]
    jdbcCh <- ZIO.service[jdbcChSession]
    oraSess <- ZIO.service[jdbcSession]
    sess <- oraSess.sessTask("startTask")
    taskId <- sess.getTaskIdFromSess
    t <- sess.getTables(newtask.schemas)
    wstask = WsTask(id = taskId,
      state = TaskState(Ready, Option.empty[Table]),
      oraServer = Some(newtask.servers.oracle),
      clickhouseServer = Some(newtask.servers.clickhouse),
      mode = newtask.servers.config,
      tables = t)
    _ <- repo.create(wstask)
    stateBefore <- repo.getState
    _ <- repo.setState(TaskState(Executing))
    stateAfter <- repo.getState
    taskId <- repo.getTaskId
    _ <- ZIO.logDebug(s"[startTask] [${taskId}] State: ${stateBefore.state} -> ${stateAfter.state} ")
    taskId <- repo.getTaskId
    sessCh <- jdbcCh.sess(taskId)
    task <- repo.ref.get
    setSchemas = task.tables.map(_.schema).toSet diff Set("system","default","information_schema")

    _ <- sess.saveTableList(task.tables)
    _ <- sessCh.createDatabases(setSchemas)
    _ <- sess.setTaskState("executing")
    fetch_size = task.oraServer.map(_.fetch_size).getOrElse(1000)
    batch_size = task.clickhouseServer.map(_.batch_size).getOrElse(1000)

    copyEffects = task.tables.map { table =>
      sess.setTableBeginCopy(table) *>
        sessCh.getMaxColForSync(table).flatMap { maxValAndCnt =>
            updatedCopiedRowsCount(table, sess, sessCh, maxValAndCnt).repeat(Schedule.spaced(5.second)).fork *>
            sessCh.recreateTableCopyData(
                table,
                sess.getDataResultSet(table, fetch_size, maxValAndCnt),
                batch_size,
                maxValAndCnt)
              .flatMap {
                rc => sess.setTableCopied(table, rc)
              }
          }.catchAll{
            case e: Exception => ZIO.logError(s"catchAll in startTask - ${e.getMessage}") *>
              repo.setState(TaskState(Wait)) *>
              ZIO.fail(new Exception(e.getMessage))
          }
          .onInterrupt {
            ZIO.logError(s"recreateTableCopyData Interrupted Oracle connection is closing") //*>
              //sess.closeConnection
          }
    }
    _ <- ZIO.collectAll(copyEffects)
    _ <- repo.setState(TaskState(Wait))
    _ <- repo.clearTask
    _ <- sess.taskFinished
    //_ <- sess.closeConnection
  } yield ()

  private def requestToEntity[A](r: Request)(implicit decoder: JsonDecoder[A]): ZIO[Any, Nothing, Either[String, A]] = for {
    req <- r.body.asString.map(_.fromJson[A])
      .catchAllDefect {
        case e: Exception => ZIO.logError(s"Error[3] parsing input file with calc : ${e.getMessage}") *>
          ZIO.succeed(Left(e.getMessage))
      }
      .catchAll {
        case e: Exception => ZIO.logError(s"Error[4] parsing input file with calc : ${e.getMessage}") *>
          ZIO.succeed(Left(e.getMessage))
      }
  } yield req

  private def currStatusCheckerTask(): ZIO[ImplTaskRepo, Throwable, Unit] = {
    for {
    repo <- ZIO.service[ImplTaskRepo]
    currStatus <- repo.getState
    taskId <- repo.getTaskId
    _ <- ZIO.logInfo(s"Repo currStatus = ${currStatus.state}")
    _ <- ZIO.fail(new Exception(
      s" already running id = $taskId ,look at tables: ora_to_ch_tasks, ora_to_ch_tasks_tables "))
      .when(currStatus.state != Wait)
  } yield ()
  }

  private def currStatusCheckerCalc(): ZIO[ImplCalcRepo, Throwable, Unit] = {
    for {
    repo <- ZIO.service[ImplCalcRepo]
    currStatus <- repo.getState
    taskId <- repo.getCalcId
    _ <- ZIO.logInfo(s"Repo currStatus = ${currStatus.state}")
    _ <- ZIO.fail(new Exception(
        s" already running id = $taskId ,look at tables: ora_to_ch_views_query_log"))
      .when(currStatus.state != Wait)
  } yield ()
  }

  private def task(req: Request): ZIO[ImplTaskRepo with SessTypeEnum, Throwable, Response] = for {
    _ <- ZIO.logDebug(s"This is a debug message 1")
    u <- requestToEntity[ReqNewTask](req)
    response <- u match {
      case Left(errorString) => ZioResponseMsgBadRequest(errorString)
      case Right(newTask) =>
        for {
             repo <- ZIO.service[ImplTaskRepo]
             _ <- currStatusCheckerTask()
             layerOraConn = OraConnRepoImpl.layer(newTask.servers.oracle)
             _ <- startTask(newTask).provide(
               layerOraConn,
               ZLayer.succeed(repo),
               ZLayer.succeed(newTask.servers.oracle),
               jdbcSessionImpl.layer,
               ZLayer.succeed(newTask.servers.clickhouse) >>> jdbcChSessionImpl.layer,
               ZLayer.succeed(SessCalc)
             ).forkDaemon
             //_ <- startTask(newTask).provideLayer(layers).forkDaemon
             sched = Schedule.spaced(1.second) && Schedule.recurs(10)
             taskId <- repo.getTaskId
               .filterOrFail(_ != 0)(0.toString)
               .retryOrElse(sched, (_: String, _: (Long, Long)) =>
                 ZIO.fail(new Exception("Elapsed wait time 10 seconds of getting taskId")))
        } yield Response.json(s"""{"taskid": "$taskId"}""").status(Status.Ok)
    }
  } yield response

  private def addOnInterr[R,E,A](eff: ZIO[R,E,A],desc: String): ZIO[R,E,A] =
    eff.onInterrupt(CalcLogic.debugInterruption(desc))

  private def calcAndCopy(reqCalc: ReqCalcSrc): ZIO[ImplCalcRepo with SessTypeEnum, Throwable, Response] = for {
    repo <- ZIO.service[ImplCalcRepo]
    _ <- currStatusCheckerCalc()
    meta = CalcLogic.getCalcMeta(reqCalc)
    _ <- {meta.flatMap{meta =>
                       addOnInterr(CalcLogic.startCalculation(reqCalc,meta),"calc") *>
                       addOnInterr(CalcLogic.copyDataChOra(reqCalc,meta),"copy")
                      }
         }.provide(
      OraConnRepoImpl.layer(reqCalc.servers.oracle),
      ZLayer.succeed(repo),
      ZLayer.succeed(reqCalc.servers.oracle),
      jdbcSessionImpl.layer,
      ZLayer.succeed(reqCalc.servers.clickhouse) >>> jdbcChSessionImpl.layer,
      ZLayer.succeed(SessCalc)
    ).forkDaemon
    sched = Schedule.spaced(1.second) && Schedule.recurs(10)
    calcId <- repo.getCalcId
      .filterOrFail(_ != 0)(0.toString)
      .retryOrElse(sched, (_: String, _: (Long, Long)) =>
        ZIO.fail(new Exception("Elapsed wait time 10 seconds of getting calcId")))
  } yield Response.json(s"""{"calcId":"$calcId", "id_vq":"${reqCalc.view_query_id}"}""").status(Status.Ok)

  private def calc(req: Request): ZIO[ImplCalcRepo with SessTypeEnum, Throwable, Response] = for {
    bodyText <- req.body.asString
    _ <- ZIO.logDebug(s"calc body = $bodyText")
    reqCalcE <- requestToEntity[ReqCalcSrc](req)
    _ <- ZIO.logDebug(s"JSON = $reqCalcE")
    resp <- reqCalcE match {
      case Left(exp_str) => ZioResponseMsgBadRequest(exp_str)
      case Right(src) => calcAndCopy(src)
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

  private val routes = Routes(
    Method.POST / "task"  -> handler{(req: Request) =>
      catchCover(task(req).provideSome[ImplTaskRepo](ZLayer.succeed(SessTask)))
    },
    Method.POST / "calc"  -> handler{(req: Request) =>
      catchCover(calc(req).provideSome[ImplCalcRepo](ZLayer.succeed(SessCalc)))
    },
    Method.GET / "random" -> handler(Random.nextString(10).map(Response.text(_))),
    Method.GET / "utc"    -> handler(Clock.currentDateTime.map(s => Response.text(s.toString))),
    Method.GET / "main"   -> handler(catchCover(getMainPage)),
    Method.GET / "text"   -> handler(ZIO.succeed(Response.text("Hello World 2!"))),
    Method.GET / "json"   -> handler(ZIO.succeed(Response.json("""{"greetings": "Hello World! 2"}""")))
  )

  val app = routes.toHttpApp
}
