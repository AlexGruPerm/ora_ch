package server


import conf.OraServer
import error.ResponseMessage
import ora.{jdbcSession, jdbcSessionImpl}
import request.ReqNewTask
import task.{Ready, TaskState, WsTask}
import zio.{ZIO, _}
import zio.http.{handler, _}
import zio.json.{DecoderOps, EncoderOps}
import request.EncDecReqNewTaskImplicits._
import server.WServer.createWsTask
import table.Table

import java.io.IOException

object WServer {

  /*
  id :Int = 0,
  state: TaskState = TaskState(Wait,Option.empty[Table]),
  oraServer: Option[OraServer] = Option.empty[OraServer],
  clickhouseServer: Option[ClickhouseServer] = Option.empty[ClickhouseServer],
  mode : Mode = Mode(),
  tables: List[Table] = List.empty[Table]
  */

  private def getTableWithInfo(/*schema: String, tableName: String*/): ZIO[OraServer with jdbcSession, Exception, Unit] = for {
    jdbc <- ZIO.service[jdbcSession]
    conn <- jdbc.pgConnection()
    x <- conn.sess.
  } yield ()

  private def createWsTask(newTask: Task[ReqNewTask]):  ZIO[Any, Throwable, Int/*WsTask*/] = for {
/*    WsTask(id = 123456,
           state = TaskState(Ready,Option.empty[Table]),
           oraServer = newTask.servers.oracle,
           clickhouseServer = newTask.servers.clickhouse,
           mode = newTask.servers.config,
           tables =
             newTask.schemas.map{sch =>
               sch.map{stbls =>
                 stbls.tables.map{tbls => tbls.map{t =>

                   Table(stbls.schema,t.name)
                  }
                 }
               }
             }
          )*/
    newtask <- newTask
/*    newtaskSchemas <- newtask.schemas
    schemas <- newtaskSchemas
    tables <- schemas.tables
    table <- tables*/
    _ <- getTableWithInfo(/*schemas.schema*//*table.name*/).provide(ZLayer.succeed(newtask.servers.oracle), jdbcSessionImpl.layer)
    //wstask = WsTask(???)
    taskId <- ZIO.succeed(123456)
  } yield taskId/*wstask*/

  private def task(req: Request): ZIO[Any, Throwable, Response] = for {
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
        //getMainPage // call method here and fork
        for {
          taskId <- createWsTask(ZIO.succeed(newTask))
          //todo: RUN PROCESS of coping data here and fork task in separate fiber
        } yield Response.json(s"""{"taskid": "${taskId}"}""").status(Status.Ok)

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
