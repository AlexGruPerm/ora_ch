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
import common._
import connrepo.OraConnRepoImpl
import table.Table

import scala.collection.mutable
import java.io.IOException
import java.sql.SQLException
import java.util.concurrent.TimeUnit
import scala.annotation.nowarn

object WServer {

  private def updatedCopiedRowsCount(
    table: Table,
    oraSession: oraSessTask,
    chSession: chSess,
    maxValCnt: Option[MaxValAndCnt] // ,
    // begin: Long
  ): ZIO[Any, Exception, Long] = for {
    // start      <- Clock.currentTime(TimeUnit.MILLISECONDS)
    copiedRows <- chSession.getCountCopiedRows(table)
    rowCount   <- oraSession.updateCountCopiedRows(
                    table,
                    copiedRows - maxValCnt.map(_.CntRows).getOrElse(0L),
                    "COPY"
                  )
    // finish     <- Clock.currentTime(TimeUnit.MILLISECONDS)
  } yield rowCount

  private def updatedCopiedRowsCountFUpd(
    srcTable: Table,
    targetTable: Table,
    oraSession: oraSessTask,
    chSession: chSess
  ): ZIO[Any, SQLException, Long] = for {
    copiedRows <- chSession.getCountCopiedRows(targetTable)
    rowCount   <- oraSession.updateCountCopiedRows(srcTable, copiedRows, "CUPD")
  } yield rowCount

  private def saveError(
    sess: oraSessTask,
    errorMsg: String,
    table: Table
  ): ZIO[ImplTaskRepo, SQLException, Unit] =
    for {
      _    <- sess.setTaskErrorSaveError(errorMsg, table)
      repo <- ZIO.service[ImplTaskRepo]
      _    <- repo.setState(TaskState(Wait))
      _    <- repo.clearTask
      _    <- ZIO.logError(s"Error for table [${table.fullTableName()}] - $errorMsg")
    } yield ()

  private def getAppendKeys(
    oraSession: oraSessTask,
    chSession: chSess,
    table: Table
  ): ZIO[ImplTaskRepo, SQLException, Option[List[Any]]] = for {
    appendKeys <- table.operation match {
                    case AppendNotIn =>
                      ZIO.logDebug(
                        s"AppendNotIn for ${table.fullTableName()} with arity = ${table.syncArity()}"
                      ) *>
                        (table.syncArity() match {
                          case 1 => chSession.whereAppendInt1(table)
                          case 2 => chSession.whereAppendInt2(table)
                          case 3 => chSession.whereAppendInt3(table)
                          case _ => ZIO.none
                        }).refineToOrDie[SQLException]
                          .tapError(er =>
                            ZIO
                              .logError(
                                s"Error in one of sessCh.whereAppendIntX - ${er.getMessage}"
                              ) *>
                              saveError(oraSession, er.getMessage, table)
                          )
                    case _           => ZIO.none
                  }
  } yield appendKeys

  def debugInterruption(taskName: String) = (fibers: Set[FiberId]) =>
    for {
      fn <- ZIO.fiberId.map(_.threadName)
      _  <- ZIO.debug(
              s"The $fn fiber which is the underlying fiber of the $taskName task " +
                s"interrupted by ${fibers.map(_.threadName).mkString(", ")}"
            )
    } yield ()

  // @nowarn
  private def copyTableEffect(
    chUpdateSession: chSess,
    taskId: Int,
    oraSession: oraSessTask,
    chLoadSession: chSess,
    table: Table,
    fetch_size: Int,
    batch_size: Int
  ): ZIO[ImplTaskRepo, Throwable, Long] = for {
    _            <- ZIO.logInfo(
                      s"Begin copyTableEffect for ${table.name} [${table.recreate}][${table.operation}]"
                    )
    _            <- oraSession.setTableBeginCopy(table)
    updateSessSid = oraSession.getPid
    _            <- ZIO.logInfo(s"For table ${table.name} update ora SID = $updateSessSid")
    // Delete rows in ch before getMaxColForSync.
    _            <- chUpdateSession
                      .deleteRowsFromChTable(table)
                      .when(
                        table.recreate == 0 &&
                          table.operation == AppendWhere
                      )

    maxValAndCnt        <- chUpdateSession.getMaxColForSync(table)
    appendKeys          <- getAppendKeys(oraSession, chUpdateSession, table)
    start               <- Clock.currentTime(TimeUnit.MILLISECONDS)
    createChTableScript <- oraSession.getCreateScript(table).when(table.operation == Recreate)
    _                   <-
      ZIO.logDebug(
        s"syncColMax = ${table.sync_by_column_max} syncUpdateColMax = ${table.sync_update_by_column_max}"
      )

    fiberUpdCnt = updatedCopiedRowsCount(table, oraSession, chUpdateSession, maxValAndCnt)
                    .delay(5.second)
                    .repeat(Schedule.spaced(5.second))
                    .onInterrupt(
                      debugInterruption("updatedCopiedRowsCount")
                    ) // todo: remove .onInterrupt(debugInterruption

    // todo: Take update of ora_to_ch_tasks_tables table from getDataResultSet !!!
    rowCountEff =
      chLoadSession
        .recreateTableCopyData(
          table,
          batch_size,
          fetch_size,
          maxValAndCnt,
          createChTableScript,
          appendKeys
        )
        .tapError { er =>
          ZIO.logError(s"recreateTableCopyData error - ${er.getMessage}") *>
            saveError(
              oraSession,
              s"recreateTableCopyData error - ${er.getMessage}",
              table
            )
        }
        .refineToOrDie[SQLException]

    rowCount <- fiberUpdCnt.disconnect race rowCountEff
    _        <- ZIO.logInfo(s"RACE result rowCount = $rowCount")
    _        <- oraSession.setTableCopied(table, rowCount)
    finish   <- Clock.currentTime(TimeUnit.MILLISECONDS)
    _        <-
      ZIO.logInfo(
        s"copyTableEffect=[$taskId] Finished [${table.name}] rows[$rowCount] ${finish - start} ms."
      )
  } yield rowCount

  /**
   * This version update column(s) with using
   *
   * CREATE TABLE UPD_TABLE ( ) ENGINE = JOIN(ANY, LEFT, PRIMARYKEYCOLUMNSCH) SETTINGS
   * JOIN_USE_NULLS = 1
   *
   * ALTER TABLE TARGET_TABLE_FOR_UPDATE UPDATE .. = JOINGET(UPD_TABLE... WHERE 1>0;
   *
   * ***** Use it ONLY when whole table updated (all rows) ***** Otherwise: For example with
   * ("where_filter":" 1>2 ") all updated fields will be updated to NULL.
   *
   * If only part of rows updated then use updateWithTableDict
   */
  /*  private def updateTableColumns(
    sessForUpd: oraSessTask,
    sess: oraSessTask,
    sessCh: chSess,
    table: Table,
    fetch_size: Int,
    batch_size: Int
  ): ZIO[ImplTaskRepo, Throwable, Long] = for {
    _                   <- sess.setTableBeginCopy(table)
    leftJoinUpdateTable  = s"upd_${table.name}"
    primaryKeyColumnsCh <- sessCh.getPkColumns(table)
    _                   <-
      updatedCopiedRowsCountFUpd(table, table.copy(name = leftJoinUpdateTable), sessForUpd, sessCh)
        .delay(2.second)
        .repeat(Schedule.spaced(5.second))
        .fork
    rowCount            <- sessCh.recreateTmpTableForUpdate(
                             table,
                             sess.getDataResultSetForUpdate(table, fetch_size, primaryKeyColumnsCh),
                             batch_size,
                             primaryKeyColumnsCh,
                             LeftJoin
                           )
    _                   <- sess.setTableCopied(table, rowCount)
    _                   <- sessCh.updateLeftJoin(table, table.copy(name = leftJoinUpdateTable), primaryKeyColumnsCh)
  } yield rowCount*/

  /**
   * Generally we update just subset of rows in table. In this case update done with DICTIONARY.
   *
   * Like in function updateTableColumns: 1) drop DICTIONARY if exists schema.dict_table 2) drop
   * table if exists schema.upd_table 3) Create MergeTree table upd_table 4) Create create
   * dictionary schema.dict_xxx 5) insert from oracle into insert into schema.upd_xxx 6) ALTER TABLE
   * schema.xxx UPDATE ... = dictGet('schema.dict_xxx', 'val', (pk_columns_in_ch)) WHERE
   * dictHas('schema.dict_xxx', (pk_columns_in_ch));
   */
  private def updateWithTableDict(
                                   chUpdateSession: chSess,
                                   @nowarn taskId: Int,
                                   oraSession: oraSessTask,
                                   chLoadSession: chSess,
                                   table: Table,
                                   fetch_size: Int,
                                   batch_size: Int
  ): ZIO[ImplTaskRepo, Throwable, Long] = for {
    _                   <- oraSession.setTableBeginCopy(table)
    updateMergeTreeTable = s"upd_${table.name}"
    updateDictName       = s"dict_${table.name}"

    updateStructsScripts <- oraSession.getUpdTblDictScripts(table)

    primaryKeyColumnsCh <- chUpdateSession.getPkColumns(table)
    fiberUpdCnt         = updatedCopiedRowsCountFUpd(
                                        table,
                                        table.copy(name = updateMergeTreeTable),
                                        oraSession,
                                        chUpdateSession
                                      )
        .delay(5.second)
        .repeat(Schedule.spaced(5.second))
      .onInterrupt(
        debugInterruption("updatedCopiedRowsCountFUpd")
      )
    loadUpdDataEff = chLoadSession.prepareStructsForUpdate(
                             updateStructsScripts,
                             table,
                             batch_size,
                             fetch_size,
                             primaryKeyColumnsCh
                           ).tapError {er =>
        ZIO.logError(s"prepareStructsForUpdate error - ${er.getMessage}") *>
          saveError(
            oraSession,
            s"prepareStructsForUpdate error - ${er.getMessage}",
            table
          )
      }
      .refineToOrDie[SQLException]

    rowCount <- fiberUpdCnt.disconnect race loadUpdDataEff

    _                   <- oraSession.setTableCopied(table, rowCount)
    _                   <- chLoadSession.updateMergeTree(table, primaryKeyColumnsCh)

    //todo: open it, close for development purpose
    /*_                   <- oraSession
                             .clearOraTable(table.clr_ora_table_aft_upd.getOrElse("xxx.yyy"))
                             .when(table.clr_ora_table_aft_upd.nonEmpty)
    */
  } yield rowCount

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
    start                   <- Clock.currentTime(TimeUnit.MILLISECONDS)
    oraSessTask             <- oraSess
                                 .sessTask()
                                 .tapError(er =>
                                   ZIO.logError(s"startTask sessTask error: ${er.getMessage} ")
                                 ) // insert into ora_to_ch_tasks
    finish                  <- Clock.currentTime(TimeUnit.MILLISECONDS)
    taskId                  <- oraSessTask.getTaskIdFromSess
    _                       <- ZIO.logDebug(s"taskId = $taskId inserted in ora_to_ch_tasks DURR: ${finish - start} ms.")
    repo                    <- ZIO.service[ImplTaskRepo]
    _                       <- repo.setTaskId(taskId)
    clickhouse              <- ZIO.service[jdbcChSession]
    chPool                  <- clickhouse.getClickHousePool()
    t                       <- oraSessTask.getTables(newtask.schemas)
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
    commonChSession         <- ZIO.succeed(chSess(chPool.getConnection, taskId))
    task                    <- repo.ref.get
    setSchemas               = task.tables.map(_.schema).toSet diff Set("system", "default", "information_schema")
    _                       <- oraSessTask.saveTableList(task.tables).tapSomeError { case er: SQLException =>
                                 ZIO.logError(s"saveTableList ${er.getMessage}") *> repo.setState(TaskState(Wait))
                               }
    _                       <- commonChSession.createDatabases(setSchemas)
    _                       <- oraSessTask.setTaskState("executing")
    fetch_size               = task.oraServer.map(_.fetch_size).getOrElse(1000)
    batch_size               = task.clickhouseServer.map(_.batch_size).getOrElse(1000)
    // sessForUpd              <- oraSess.sessTask(Some(taskId))
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
                                             chSess(
                                               chPool.getConnection,
                                               taskId
                                             ), // ch connect for update
                                             task.id,
                                             s,
                                             chSess(
                                               chPool.getConnection,
                                               taskId
                                             ), // ch connect for load data
                                             table,
                                             fetch_size,
                                             batch_size
                                           )
                                         case _                                                  => ZIO.succeed(0L)
                                       }
                                     )
                                       .refineToOrDie[SQLException] *>
                                       closeSession(s, table)
                                     // todo: Explicitly interrupt internal effect inside, before closing session.
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
                                         // updateTableColumns - for update whole table (all rows).
                                         updateWithTableDict(
                                           chSess(
                                             chPool.getConnection,
                                             taskId
                                           ), // ch connect for update
                                           task.id,
                                           s,
                                           chSess(
                                             chPool.getConnection,
                                             taskId
                                           ), // ch connect for load data
                                           table,
                                           fetch_size,
                                           batch_size
                                         )
                                       case _      => ZIO.succeed(0L)
                                     }
                                   )
                                     .refineToOrDie[SQLException] *>
                                     closeSession(s, table)
                                 }
                               }

    _ <- if (wstask.parDegree <= 3) {
           // It's possible to have inserts,appendsXXX operations together with updates
           // Execute it with necessary order.
           ZIO.logInfo(
             s"Data modification SEQUENTIALLY with ${wstask.parDegree}"
           ) *>
             ZIO.collectAll(operationsExcludeUpdates) *>
             ZIO.collectAll(operationsUpdates)
         } else
           ZIO.logInfo(
             s"Data modification PARALLEL with ${wstask.parDegree}"
           ) *>
             ZIO.collectAllPar(operationsExcludeUpdates).withParallelism(wstask.parDegree - 1) *>
             ZIO.collectAllPar(operationsUpdates).withParallelism(wstask.parDegree - 1)
    _ <- ZIO.logInfo(s"startTask FINISH oraSession.SID = ${oraSessTask.getPid} will be closed.")
    _ <- oraSessTask.taskFinished
    _ <- ZIO.attemptBlockingInterrupt {
           // sessForUpd.sess.commit()
           // sessForUpd.sess.close()
           oraSessTask.sess.commit()
           oraSessTask.sess.close()
         }
    _ <- repo.setState(TaskState(Wait))
    _ <- repo.clearTask
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

    /*    exitValue <- taskFiber.await.timeout(30.seconds) //Fuck off
    _         <- exitValue match {
                   case Some(ex) =>
                     ex match {
                       case Exit.Success(_)     =>
                         ZIO.logInfo(s"startTask completed successfully.")
                       case Exit.Failure(cause) =>
                         ZIO.logError(s"**** Fiber failed ****") *>
                           ZIO.logError(s"${cause.prettyPrint}") *>
                           ZIO.fail(
                             new Exception(
                               s"Fiber failed with: ${cause.map(th => th.getMessage)}"
                             )
                           )
                     }
                   case None     => ZIO.fail(new Exception("Timeout taskFiber.await"))
                 }
    _         <- taskFiber.interrupt*/
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

  private def getCalcAndCopyEffect(
    oraSess: jdbcSession,
    q: Query,
    idReloadCalcId: Int
  ): ZIO[ImplCalcRepo with jdbcChSession with jdbcSession, Throwable, Unit] = for {
    s          <- oraSess.sessCalc(debugMsg = "calc - copyDataChOra")
    meta       <- CalcLogic.getCalcMeta(q, s)
    queryLogId <- CalcLogic.startCalculation(q, meta, s, idReloadCalcId)
    eff        <- CalcLogic.copyDataChOra(q, meta, s, queryLogId)
  } yield eff

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
        query.map(q => (q.query_id, getCalcAndCopyEffect(oraSess, q, reqCalc.id_reload_calc)))
      )

    /*    // old code
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
      )*/

    _ <- ZIO.foreachDiscard(calcsEffects) { lst =>
           ZIO.logInfo(s"parallel execution of ${lst.map(_._1).toList} ---------------") *>
             ZIO
               .collectAllPar(lst.map(_._2))
               .withParallelism(2 /*SEQ-PAR*/ )
               .tapError(_ => repo.clearCalc)
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
                OraConnRepoImpl.layer(reqCalc.servers.oracle, 2),
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
