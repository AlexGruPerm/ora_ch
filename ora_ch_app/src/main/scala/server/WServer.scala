package server

import calc.{ CalcLogic, ImplCalcRepo, Query, ReqCalc, ReqCalcSrc }
import clickhouse.{ chSess, jdbcChSession, jdbcChSessionImpl }
import common.Types.MaxValAndCnt
import error.ResponseMessage
import ora.{ jdbcSession, jdbcSessionImpl, oraSessTask }
import request.{ AppendByMax, AppendNotIn, AppendWhere, Parallel, Recreate, ReqNewTask, Update }
import task.{ ImplTaskRepo, WsTask }
import zio._
import zio.http._
import zio.json.{ DecoderOps, EncoderOps, JsonDecoder }
import request.EncDecReqNewTaskImplicits._
import calc.EncDecReqCalcImplicits._
import common.{ SessCalc, SessTask, SessTypeEnum, TaskState, Wait, _ }
import connrepo.OraConnRepoImpl
import table.Table

import scala.collection.mutable
import java.io.IOException
import java.sql.SQLException

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
    updateFiber: Option[Fiber.Runtime[Exception, Long]],
    table: Table
  ): ZIO[ImplTaskRepo, SQLException, Unit] =
    for {
      _    <- sess.setTaskError(errorMsg, table)
      repo <- ZIO.service[ImplTaskRepo]
      _    <- repo.setState(TaskState(Wait))
      _    <- repo.clearTask
      _    <- ZIO.logError(s"Error for table [${table.fullTableName()}] - $errorMsg")
      _    <- if (updateFiber.nonEmpty)
                updateFiber.get.interrupt
              else
                ZIO.unit
      // updateFiber.get.interrupt.when(updateFiber.nonEmpty)
    } yield ()

  private def getAppendKeys(
    sess: oraSessTask,
    sessCh: chSess,
    table: Table
  ): ZIO[ImplTaskRepo, SQLException, Option[List[Any]]] = for {
    appendKeys <- table.operation match {
                    case AppendNotIn =>
                      ZIO.logDebug(
                        s"AppendNotIn for ${table.fullTableName()} with arity = ${table.syncArity()}"
                      ) *>
                        (table.syncArity() match {
                          case 1 => sessCh.whereAppendInt1(table)
                          case 2 => sessCh.whereAppendInt2(table)
                          case 3 => sessCh.whereAppendInt3(table)
                          case _ => ZIO.succeed(None)
                        }).refineToOrDie[SQLException]
                          .tapError(er =>
                            ZIO
                              .logError(
                                s"Error in one of sessCh.whereAppendIntX - ${er.getMessage}"
                              ) *>
                              saveError(sess, er.getMessage, None, table)
                          )
                    case _           => ZIO.succeed(None)
                  }
  } yield appendKeys

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
    // Delete rows in ch before getMaxColForSync.
    _            <- sessCh
                      .deleteRowsFromChTable(table)
                      .when(
                        table.recreate == 0 &&
                          table.operation == AppendWhere
                      )
    maxValAndCnt <- sessCh.getMaxColForSync(table)
    _            <- ZIO.logDebug(s"maxValAndCnt = $maxValAndCnt")
    appendKeys   <- getAppendKeys(sess, sessCh, table)
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
                          saveError(sess, er.getMessage, Some(fiberUpdCnt), table)
                      )
                      .refineToOrDie[SQLException]
    _            <- fiberUpdCnt.interrupt
    _            <- sess.setTableCopied(table, rowCount)
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
              sess.setTableCopied(table, rowCount) *>
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
    _                       <- ZIO.logDebug(s" newtask = $newtask")
    oraSess                 <- ZIO.service[jdbcSession]
    repo                    <- ZIO.service[ImplTaskRepo]
    jdbcCh                  <- ZIO.service[jdbcChSession]
    sess                    <- oraSess.sessTask()
    taskId                  <- sess.getTaskIdFromSess
    _                       <- repo.setTaskId(taskId)
    t                       <- sess.getTables(newtask.schemas)
    wstask                   = WsTask(
                                 id = taskId,
                                 state = TaskState(Ready, Option.empty[Table]),
                                 oraServer = Some(newtask.servers.oracle),
                                 clickhouseServer = Some(newtask.servers.clickhouse),
                                 parallel = newtask.parallel,
                                 tables = t,
                                 newtask.parallel.degree
                               )
    _                       <- repo.create(wstask)
    _                       <- repo.setState(TaskState(Executing))
    taskId                  <- repo.getTaskId
    sessCh                  <- jdbcCh.sess(taskId)
    task                    <- repo.ref.get
    setSchemas               = task.tables.map(_.schema).toSet diff Set("system", "default", "information_schema")
    _                       <- sess.saveTableList(task.tables).tapSomeError { case er: SQLException =>
                                 ZIO.logError(s"saveTableList ${er.getMessage}") *> repo.setState(TaskState(Wait))
                               }
    _                       <- sessCh.createDatabases(setSchemas)
    _                       <- sess.setTaskState("executing")
    fetch_size               = task.oraServer.map(_.fetch_size).getOrElse(1000)
    batch_size               = task.clickhouseServer.map(_.batch_size).getOrElse(1000)
    // All operations excluding Update(s), updates must be executed after recreate and appends.
    operationsExcludeUpdates = task.tables.filter(_.operation != Update).map { table =>
                                 ZIO.logInfo(
                                   s"OPERATION [${table.operation}] FOR ${table.fullTableName()}"
                                 ) *>
                                   oraSess.sessTask(Some(taskId)).flatMap { s =>
                                     (
                                       table.operation match {
                                         // Later we can use matching here for adjustment logic.
                                         case Recreate | AppendWhere | AppendByMax | AppendNotIn =>
                                           copyTableEffect(
                                             task.id,
                                             s,
                                             sessCh,
                                             table,
                                             fetch_size,
                                             batch_size
                                           )
                                         case _                                                  => ZIO.succeed(0L)
                                       }
                                     )
                                       .refineToOrDie[SQLException] *>
                                       closeSession(s, table)
                                   }
                               }

    // Only Updates
    operationsUpdates        = task.tables.filter(_.operation == Update).map { table =>
                                 ZIO.logInfo(
                                   s"OPERATION [${table.operation}] FOR ${table.fullTableName()}"
                                 ) *> oraSess.sessTask(Some(taskId)).flatMap { s =>
                                   (
                                     table.operation match {
                                       case Update =>
                                         updateTableColumns(s, sessCh, table, fetch_size, batch_size)
                                       case _      => ZIO.succeed(0L)
                                     }
                                   )
                                     .refineToOrDie[SQLException] *>
                                     closeSession(s, table)
                                 }
                               }

    _ <- if (wstask.parDegree <= 2)
           ZIO.logInfo(
             s"copyEffectsExcludeUpdates SEQUENTIALLY with parDegree = ${wstask.parDegree}"
           ) *>
             ZIO.collectAll(operationsExcludeUpdates)
         else
           ZIO.logInfo(
             s"copyEffectsExcludeUpdates IN PARALLEL with parDegree = ${wstask.parDegree}"
           ) *>
             ZIO.collectAllPar(operationsExcludeUpdates).withParallelism(wstask.parDegree - 1)

    _ <-
      if (wstask.parDegree <= 2)
        ZIO.logInfo(s"copyEffectsOnlyUpdates SEQUENTIALLY with parDegree = ${wstask.parDegree}") *>
          ZIO.collectAll(operationsUpdates)
      else
        ZIO.logInfo(s"copyEffectsOnlyUpdates IN PARALLEL with parDegree = ${wstask.parDegree}") *>
          ZIO.collectAllPar(operationsUpdates).withParallelism(wstask.parDegree - 1)

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
      // taskId     <- repo.getCalcId
      _          <- ZIO.logInfo(s"Repo currStatus = ${currStatus.state}")
      _          <- ZIO
                      .fail(
                        new Exception(
                          s" already running, look at tables: ora_to_ch_query_log"
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
                      layerOraConnRepo = // plus1 - 1 session for updates.
                        OraConnRepoImpl.layer(
                          newTask.servers.oracle,
                          newTask.parallel.plus1,
                          "Ucp_task"
                        )
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

  private def listToQueue[A](l: List[A]): mutable.Queue[A] = new mutable.Queue[A] ++= l

  private def mapOfQueues(queries: List[Query]): Map[Int, mutable.Queue[Query]] =
    queries.groupBy(_.query_id).map { case (k: Int, lst: List[Query]) =>
      (k, listToQueue(lst))
    }

  def listOfListsQuery(
    m: Map[Int, mutable.Queue[Query]],
    acc: List[List[Query]] = List.empty[List[Query]]
  ): List[List[Query]] =
    if (m.exists(_._2.nonEmpty)) {
      if (m.count(_._2.nonEmpty) == 1) {
        val nonemptyQueue = m.find(_._2.nonEmpty)
        nonemptyQueue match {
          case Some((key, queue)) =>
            val element: Query = queue.dequeue()
            listOfListsQuery(Map(key -> queue), acc :+ List(element))
          case None               => acc
        }
      } else {
        val keys: List[Int]          = m.filter(_._2.nonEmpty).keys.toList
        val k1: Int                  = keys.head
        val k2: Int                  = keys.tail.head
        val q1: mutable.Queue[Query] = m.getOrElse(k1, mutable.Queue.empty[Query])
        val q2: mutable.Queue[Query] = m.getOrElse(k2, mutable.Queue.empty[Query])
        val query1                   = q1.dequeue()
        val query2                   = q2.dequeue()
        listOfListsQuery(m.updated(k1, q1).updated(k2, q2), acc :+ List(query1, query2))
      }
    } else
      acc

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
        query.map(q =>
          (
            q.query_id,
            oraSess.sessCalc(debugMsg = "calc - copyDataChOra").flatMap { s =>
              CalcLogic.getCalcMeta(q, s).flatMap { meta =>
                CalcLogic.startCalculation(q, meta, s, reqCalc.id_reload_calc).flatMap {
                  queryLogId =>
                    CalcLogic.copyDataChOra(q, meta, s, queryLogId)
                }
              }
            }
          )
        )
      )

    _ <- ZIO.foreachDiscard(calcsEffects) { lst =>
           ZIO.logInfo(s"parallel execution of ${lst.map(_._1).toList} ---------------") *>
             ZIO.collectAllPar(lst.map(_._2)).withParallelism(2)
         }

    _ <- repo.clearCalc
  } yield ()

  private def calcAndCopy(
    reqCalc: ReqCalcSrc // ,
    // waitSeconds: Int
  ): ZIO[ImplCalcRepo with SessTypeEnum, Throwable, Response] = for {
    repo <- ZIO.service[ImplCalcRepo]
    _    <- currStatusCheckerCalc()
    _    <- repo.create(ReqCalc(id = 111222333)) // todo: put here ora_to_ch_reload_calc.ID
    _    <- repo.setState(CalcState(Executing))
    /**
     * We can use parallel execution only with degree =2. Because listOfListsQuery(mapOfQueues
     * algorithm make List of lists Query, List.size=2 Parallel(degree = 2)
     */
    _    <- executeCalcAndCopy(reqCalc)
              .provide(
                OraConnRepoImpl.layer(reqCalc.servers.oracle, Parallel(degree = 2), "Ucp_calc"),
                ZLayer.succeed(repo),
                ZLayer.succeed(reqCalc.servers.oracle) >>> jdbcSessionImpl.layer,
                ZLayer.succeed(reqCalc.servers.clickhouse) >>> jdbcChSessionImpl.layer,
                ZLayer.succeed(SessCalc)
              )
              .forkDaemon

    /* sched   = Schedule.spaced(1.second) && Schedule.recurs(waitSeconds)
    calcId <-
      repo
        .getCalcId
        .filterOrFail(_ != 0)(0.toString)
        .retryOrElse(
          sched,
          (_: String, _: (Long, Long)) =>
            ZIO.fail(new Exception(s"Elapsed wait time $waitSeconds seconds of getting calcId"))
        )*/
  } yield Response.json(s"""{"calcId":"ok"}""").status(Status.Ok)

  private def calc(
    req: Request // ,
    // waitSeconds: Int
  ): ZIO[ImplCalcRepo with SessTypeEnum, Throwable, Response] = for {
    // bodyText <- req.body.asString
    // _        <- ZIO.logDebug(s"calc body = $bodyText")
    reqCalcE <- requestToEntity[ReqCalcSrc](req)
    _        <- ZIO.logDebug(s"JSON = $reqCalcE")
    resp     <- reqCalcE match {
                  case Left(exp_str) => ZioResponseMsgBadRequest(exp_str)
                  case Right(src)    => calcAndCopy(src /*, waitSeconds*/ )
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
        catchCover(calc(req /*, waitSeconds*/ ).provideSome[ImplCalcRepo](ZLayer.succeed(SessCalc)))
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
