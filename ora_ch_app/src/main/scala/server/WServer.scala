package server


import clickhouse.{jdbcChSession, jdbcChSessionImpl}
import conf.OraServer
import error.ResponseMessage
import ora.{jdbcSession, jdbcSessionImpl}
import request.{ReqNewTask, SrcTable}
import task.{Executing, ImplTaskRepo, Ready, TaskState, Wait, WsTask}
import zio.{ZIO, _}
import zio.http.{handler, _}
import zio.json.{DecoderOps, EncoderOps}
import request.EncDecReqNewTaskImplicits._
import table.Table

import java.io.IOException
import scala.collection.immutable.List

object WServer {

  private def createWsTask(newTask: Task[ReqNewTask]):  ZIO[OraServer with jdbcSession, Throwable, WsTask] = for {
    jdbc <- ZIO.service[jdbcSession]
    sess <- jdbc.sess
    taskId <- sess.getTaskIdFromSess
    newtask <- newTask
    t <- sess.getTables(newtask.schemas)
    wstask = WsTask(id = taskId,
      state = TaskState(Ready, Option.empty[Table]),
      oraServer = Some(newtask.servers.oracle),
      clickhouseServer = Some(newtask.servers.clickhouse),
      mode = newtask.servers.config,
      tables = t)
  } yield wstask


  private def startTask(wstask: WsTask) : ZIO[ImplTaskRepo with OraServer with jdbcSession with jdbcChSession, Throwable, Unit] = for {
    repo <- ZIO.service[ImplTaskRepo]
    stateBefore <- repo.getState()
    _ <- repo.setState(TaskState(Executing))
    stateAfter <- repo.getState()
    _ <- ZIO.logInfo(s" [startTask] [${wstask.id}] State: ${stateBefore} -> ${stateAfter} ")
    jdbc <- ZIO.service[jdbcSession] //todo: don't call it, new session
    sess <- jdbc.sess
    _ <- ZIO.logInfo(s"[startTask] Oracle session taskId = ${sess.taskId}")
    jdbcCh <- ZIO.service[jdbcChSession]
    sessCh <- jdbcCh.sess(sess.taskId)
    _ <- ZIO.logInfo(s"[startTask] Clickhouse session taskId = ${sessCh.taskId}")

  } yield ()

  private def task(req: Request): ZIO[ImplTaskRepo, Throwable, Response] = for {
    bodyText <- req.body.asString
     _ <- ZIO.logInfo(s"task body = $bodyText")

    u <- req.body.asString.map(_.fromJson[ReqNewTask])
      .catchAllDefect {
        case e: Exception => ZIO.logError(s"oratoch-1 error parsing input file with task : ${e.getMessage}") *>
          ZIO.succeed(Left(e.getMessage))
      }
      .catchAll {
        case e: Exception => ZIO.logError(s"oratoch-2 error parsing input file with task : ${e.getMessage}") *>
          ZIO.succeed(Left(e.getMessage))
      }

    _ <- ZIO.logInfo("~~~~~~~~~~~~~~~~~~")
    _ <- ZIO.logInfo(s"JSON = $u")
    _ <- ZIO.logInfo("~~~~~~~~~~~~~~~~~~")

    resp <- u match {
      case Left(exp_str) => ZioResponseMsgBadRequest(exp_str)
      case Right(newTask) =>
        for {
          wstask <- createWsTask(ZIO.succeed(newTask)).provide(ZLayer.succeed(newTask.servers.oracle), jdbcSessionImpl.layer)
          tr <- ZIO.service[ImplTaskRepo]
          _ <- startTask(wstask).provide(ZLayer.succeed(tr),
                                   ZLayer.succeed(newTask.servers.oracle),
                                   jdbcSessionImpl.layer,
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
    Method.POST / "task" -> handler{(req: Request) => catchCover(task(req))},
    Method.GET / "random" -> handler(Random.nextString(10).map(Response.text(_))),
    Method.GET / "utc"    -> handler(Clock.currentDateTime.map(s => Response.text(s.toString))),
    Method.GET / "main"   -> handler(catchCover(getMainPage)),
    Method.GET / "text"   -> handler(ZIO.succeed(Response.text("Hello World 2!"))),
    Method.GET / "json"   -> handler(ZIO.succeed(Response.json("""{"greetings": "Hello World! 2"}""")))
  )

  val app = routes.toHttpApp
}
