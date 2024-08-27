package server

import calc.{ CalcLogic, ImplCalcRepo, ReqCalcSrc }
import clickhouse.jdbcChSessionImpl
import error.ResponseMessage
import ora.jdbcSessionImpl
import request.ReqNewTask
import task.{ ImplTaskRepo, TaskLogic }
import zio._
import zio.http._
import zio.json.{ DecoderOps, EncoderOps, JsonDecoder }
import request.EncDecReqNewTaskImplicits._
import calc.EncDecReqCalcImplicits._
import common._
import connrepo.OraConnRepoImpl
import java.io.IOException

object WServer {

  private def requestToEntity[A](
    r: Request
  )(implicit decoder: JsonDecoder[A]): ZIO[Any, Nothing, Either[String, A]] = for {
    req <- r.body
             .asString
             .map(_.fromJson[A])
             .catchAllDefect { case e: Exception =>
               ZIO
                 .logError(s"Error[3] parsing input file with : ${e.getMessage}")
                 .as(Left(e.getMessage))
             }
             .catchAll { case e: Exception =>
               ZIO
                 .logError(s"Error[4] parsing input file with : ${e.getMessage}")
                 .as(Left(e.getMessage))
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

  /**
   * When task is started as startTask(newTask).provide(...).forkDaemon we want save error in oracle
   * db when fiber fail or die. This catcher take this error and save it into db.
   */
  private def errorCatcherForkedTask(
    taskEffect: ZIO[Any, Throwable, Unit]
  ): ZIO[Any, Throwable, Unit] = for {
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
                        OraConnRepoImpl.layer(newTask.servers.oracle, newTask.parallel.degree)
                      // There is no connection created.
                      taskEffect       = TaskLogic
                                           .startTask(newTask)
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

  private def calc(
    req: Request
  ): ZIO[ImplCalcRepo with SessTypeEnum, Throwable, Response] = for {
    reqCalcE <- requestToEntity[ReqCalcSrc](req)
    _        <- ZIO.logDebug(s"JSON = $reqCalcE")
    resp     <- reqCalcE match {
                  case Left(exp_str) => ZioResponseMsgBadRequest(exp_str)
                  case Right(src)    => CalcLogic.calcAndCopy(src)
                }
  } yield resp

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
      Method.POST / "task" -> handler { (req: Request) =>
        catchCover(task(req, waitSeconds).provideSome[ImplTaskRepo](ZLayer.succeed(SessTask)))
      },
      Method.POST / "calc" -> handler { (req: Request) =>
        catchCover(calc(req).provideSome[ImplCalcRepo](ZLayer.succeed(SessCalc)))
      }
    )

  val app: Int => HttpApp[ImplTaskRepo with ImplCalcRepo] = waitSeconds =>
    routes(waitSeconds).toHttpApp
}
