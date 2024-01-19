package server


import calc.{CalcLogic, ReqCalc}
import clickhouse.{chSess, jdbcChSession, jdbcChSessionImpl}
import common.Types.MaxValAndCnt
import conf.OraServer
import error.ResponseMessage
import ora.{jdbcSession, jdbcSessionImpl, oraSess}
import request.{ReqNewTask, SrcTable}
import task.{Executing, ImplTaskRepo, Ready, TaskState, Wait, WsTask}
import zio.{ZIO, _}
import zio.http.{handler, _}
import zio.json.{DecoderOps, EncoderOps, JsonDecoder}
import request.EncDecReqNewTaskImplicits._
import calc.EncDecReqCalcImplicits._
import table.Table
import zio.json.JsonDecoder.fromCodec

import java.io.IOException
import scala.collection.immutable.List

object WServer {

  private def createWsTask(newtask: ReqNewTask): ZIO[OraServer with jdbcSession, Throwable, Tuple2[WsTask,oraSess]] = for {
    jdbc <- ZIO.service[jdbcSession]
    sess <- jdbc.sess
    taskId <- sess.getTaskIdFromSess
    t <- sess.getTables(newtask.schemas)
    wstask = WsTask(id = taskId,
      state = TaskState(Ready, Option.empty[Table]),
      oraServer = Some(newtask.servers.oracle),
      clickhouseServer = Some(newtask.servers.clickhouse),
      mode = newtask.servers.config,
      tables = t)
  } yield (wstask,sess)

  private def updatedCopiedRowsCount(table: Table,ora: oraSess, ch: chSess, maxValCnt: Option[MaxValAndCnt]): ZIO[Any,Nothing,Unit] = for {
    copiedRows <- ch.getCountCopiedRows(table)
    //_ <- ZIO.logInfo(s"updatedCopiedRowsCount copiedRows=$copiedRows ..............................")
    _ <- ora.updateCountCopiedRows(table,copiedRows - maxValCnt.map(_.CntRows).getOrElse(0L))
  } yield ()

  private def startTask(sess: oraSess): ZIO[ImplTaskRepo with jdbcChSession, Throwable, Unit] = for {
    repo <- ZIO.service[ImplTaskRepo]
    stateBefore <- repo.getState
    _ <- repo.setState(TaskState(Executing))
    stateAfter <- repo.getState
    taskId <- repo.getTaskId
    _ <- ZIO.logInfo(s"[startTask] [${taskId}] State: ${stateBefore} -> ${stateAfter} ")
    taskId <- repo.getTaskId
    jdbcCh <- ZIO.service[jdbcChSession]
    sessCh <- jdbcCh.sess(taskId)
    _ <- ZIO.logInfo(s"[startTask] Clickhouse session taskId = ${sessCh.taskId}")
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
          }
          .onInterrupt {
            ZIO.logError(s"recreateTableCopyData Interrupted Oracle connection is closing") *>
              sess.closeConnection
          }
    }
    /*
    copyEffects = task.tables.map{table =>
      sess.setTableBeginCopy(table) *>
        updatedCopiedRowsCount(table,sess,sessCh).repeat(Schedule.spaced(5.second)).fork *>
            sessCh.getMaxColForSync(table).flatMap{maxValAndCnt =>
                sessCh.recreateTableCopyData(
                    table,
                    sess.getDataResultSet(table, fetch_size, maxValAndCnt),
                    batch_size,
                    maxValAndCnt)
                  .flatMap {
                  rc => sess.setTableCopied(table, rc)
                }
            }
            .onInterrupt{
               ZIO.logError(s"recreateTableCopyData Interrupted Oracle connection is closing") *>
                 sess.closeConnection
             }
    }
    */
    _ <- ZIO.collectAll(copyEffects) //todo: ZIO.collectAllPar(copyEffects).withParallelism(par_degree)
    _ <- sess.taskFinished
    _ <- sess.closeConnection
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

  private def task(req: Request): ZIO[ImplTaskRepo, Throwable, Response] = for {
    bodyText <- req.body.asString
     _ <- ZIO.logInfo(s"task body = $bodyText")

    u <- requestToEntity[ReqNewTask](req)

/*    u <- req.body.asString.map(_.fromJson[ReqNewTask])
      .catchAllDefect {
        case e: Exception => ZIO.logError(s"oratoch-1 error parsing input file with task : ${e.getMessage}") *>
          ZIO.succeed(Left(e.getMessage))
      }
      .catchAll {
        case e: Exception => ZIO.logError(s"oratoch-2 error parsing input file with task : ${e.getMessage}") *>
          ZIO.succeed(Left(e.getMessage))
      }*/

    _ <- ZIO.logInfo("~~~~~~~~~~~~~~~~~~")
    _ <- ZIO.logInfo(s"JSON = $u")
    _ <- ZIO.logInfo("~~~~~~~~~~~~~~~~~~")

    resp <- u match {
      case Left(exp_str) => ZioResponseMsgBadRequest(exp_str)
      case Right(newTask) =>
        for {
          taskWithOraSess <- createWsTask(newTask).provide(ZLayer.succeed(newTask.servers.oracle), jdbcSessionImpl.layer)
          wstask = taskWithOraSess._1
          ora = taskWithOraSess._2
          tr <- ZIO.service[ImplTaskRepo]
          _ <- tr.create(wstask)
          _ <- startTask(ora).provide(ZLayer.succeed(tr),
                                   ZLayer.succeed(newTask.servers.clickhouse),
                                   jdbcChSessionImpl.layer).forkDaemon
          //_ <- fiber.join
          //todo: RUN PROCESS of coping data here and fork task in separate fiber
        } yield Response.json(s"""{"taskid": "${wstask.id}"}""").status(Status.Ok)
/*        .foldZIO(
          error => ZIO.logError(s"oratoch-3 error parsing input file with task : ${error.getMessage}") *>
            ZioResponseMsgBadRequest(error.getMessage),
          success => ZIO.succeed(success)
        )*/
    }
     // resp =  Response.html(s"HTML: $bodyText", Status.Ok)
  } yield resp

  private def calcCopy(reqCalc: ReqCalc): ZIO[ImplTaskRepo, Throwable, Response] = for {
    _ <- ZIO.unit
    calc = CalcLogic.startCalculation(reqCalc).onInterrupt(CalcLogic.debugInterruption("calc"))
    copy = CalcLogic.copyDataChOra(reqCalc).onInterrupt(CalcLogic.debugInterruption("copy"))
    layers = (ZLayer.succeed(reqCalc.servers.clickhouse) >>> jdbcChSessionImpl.layer) ++
      (ZLayer.succeed(reqCalc.servers.oracle) >>> jdbcSessionImpl.layer)
    _ <- (calc *> copy).provideLayer(layers).forkDaemon
  } yield Response.json(s"""{"calcid": "${reqCalc.view_query_id}"}""").status(Status.Ok)

  private def calc(req: Request): ZIO[ImplTaskRepo, Throwable, Response] = for {
    bodyText <- req.body.asString
    _ <- ZIO.logInfo(s"calc body = $bodyText")
    reqCalcE <- requestToEntity[ReqCalc](req)
    _ <- ZIO.logInfo(s"JSON = $reqCalcE")

    resp <- reqCalcE match {
      case Left(exp_str) => ZioResponseMsgBadRequest(exp_str)
      case Right(reqCalc) => calcCopy(reqCalc)
    }

  } yield resp


  private def getMainPage: ZIO[Any, IOException, Response] =
    ZIO.fail(new IOException("error text in IOException"))
/*    for {
      _ <- ZIO.logInfo("getMainPage 1")
      _ <- ZIO.fail(throw new IOException("error text in IOException"))
      resp <- ZIO.succeed(Response.html(s"Hello HTML example"))
      _ <- ZIO.logInfo("getMainPage 2")
    } yield resp*/

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
    Method.POST / "task"  -> handler{(req: Request) => catchCover(task(req))},
    Method.POST / "calc"  -> handler{(req: Request) => catchCover(calc(req))},
    Method.GET / "random" -> handler(Random.nextString(10).map(Response.text(_))),
    Method.GET / "utc"    -> handler(Clock.currentDateTime.map(s => Response.text(s.toString))),
    Method.GET / "main"   -> handler(catchCover(getMainPage)),
    Method.GET / "text"   -> handler(ZIO.succeed(Response.text("Hello World 2!"))),
    Method.GET / "json"   -> handler(ZIO.succeed(Response.json("""{"greetings": "Hello World! 2"}""")))
  )

  val app = routes.toHttpApp
}
