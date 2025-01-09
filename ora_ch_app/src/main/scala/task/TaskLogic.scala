package task

import clickhouse.{ chSess, jdbcChSession }
import common.Types.MaxValAndCnt
import common.{ Executing, Ready, TaskState, Wait }
import ora.{ jdbcSession, oraSessTask }
import request.{ AppendByMax, AppendNotIn, AppendWhere, Recreate, ReqNewTask, Update }
import table.Table
import zio.{ durationInt, Clock, FiberId, Schedule, ZIO }
import java.sql.SQLException
import java.util.concurrent.TimeUnit
import scala.annotation.nowarn

object TaskLogic {

  private def closeSession(s: oraSessTask, table: Table): ZIO[Any, SQLException, Unit] = for {
    _ <- ZIO.attemptBlockingInterrupt {
           s.sess.commit()
           s.sess.close()
         }.refineToOrDie[SQLException]
           .tapError(er =>
             ZIO.logError(s"closeSession [${table.fullTableName()}] error: ${er.getMessage}")
           )
  } yield ()

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
    fiberUpdCnt          = updatedCopiedRowsCountFUpd(
                             table,
                             table.copy(name = updateMergeTreeTable),
                             oraSession,
                             chUpdateSession
                           ).delay(5.second)
                             .repeat(Schedule.spaced(5.second))
                             .onInterrupt(
                               debugInterruption("updatedCopiedRowsCountFUpd")
                             )
    loadUpdDataEff       = chLoadSession
                             .prepareStructsForUpdate(
                               updateStructsScripts,
                               table,
                               batch_size,
                               fetch_size,
                               primaryKeyColumnsCh
                             )
                             .tapError { er =>
                               ZIO.logError(s"prepareStructsForUpdate error - ${er.getMessage}") *>
                                 saveError(
                                   oraSession,
                                   s"prepareStructsForUpdate error - ${er.getMessage}",
                                   table
                                 )
                             }
                             .refineToOrDie[SQLException]

    rowCount <- fiberUpdCnt.disconnect race loadUpdDataEff

    _ <- oraSession.setTableCopied(table, rowCount)
    _ <- chLoadSession.updateMergeTree(table, primaryKeyColumnsCh)

    _ <- oraSession
           .clearOraTable(table.clr_ora_table_aft_upd.getOrElse("xxx.yyy"))
           .when(table.clr_ora_table_aft_upd.nonEmpty && rowCount > 0L)

  } yield rowCount

  def updatedCopiedRowsCountFUpd(
    srcTable: Table,
    targetTable: Table,
    oraSession: oraSessTask,
    chSession: chSess
  ): ZIO[Any, SQLException, Long] = for {
    copiedRows <- chSession.getCountCopiedRows(targetTable)
    rowCount   <- oraSession.updateCountCopiedRows(srcTable, copiedRows, "CUPD")
  } yield rowCount

  private def updatedCopiedRowsCount(
    table: Table,
    oraSession: oraSessTask,
    chSession: chSess,
    maxValCnt: Option[MaxValAndCnt]
  ): ZIO[Any, Exception, Long] = for {
    copiedRows <- chSession.getCountCopiedRows(table)
    rowCount   <- oraSession.updateCountCopiedRows(
                    table,
                    copiedRows - maxValCnt.map(_.CntRows).getOrElse(0L),
                    "COPY"
                  )
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

  private def copyTableEffect(
    chUpdateSession: chSess,
    taskId: Int,
    oraSession: oraSessTask,
    chLoadSession: chSess,
    table: Table,
    fetch_size: Int,
    batch_size: Int
  ): ZIO[ImplTaskRepo, Throwable, Long] = for {
    pfn <- ZIO.fiberId.map(_.threadName)
    _            <- ZIO.logInfo(
                      s"Begin fiber=[$pfn] copyTableEffect for ${table.name} [${table.recreate}][${table.operation}]"
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
                      debugInterruption(s"updatedCopiedRowsCount[${table.name}]")
                    )

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

  def startTask(
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
           oraSessTask.sess.commit()
           oraSessTask.sess.close()
         }
    _ <- repo.setState(TaskState(Wait))
    _ <- repo.clearTask
  } yield ()

}
