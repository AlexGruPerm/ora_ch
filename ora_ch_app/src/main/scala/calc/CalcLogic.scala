package calc

import clickhouse.jdbcChSession
import ora.jdbcSession
import zio.{FiberId, ZIO}

/**
 * Contains functions that related with process of calculation in clickhouse.
*/
object CalcLogic {

  def debugInterruption(funcName: String): Set[FiberId] => ZIO[Any,Nothing,Unit] = (fibers: Set[FiberId]) =>
    for {
      fn <- ZIO.fiberId.map(_.threadName)
      _ <- ZIO.debug(
        s"The $fn fiber which is the underlying fiber of the $funcName task " +
          s"interrupted by ${fibers.map(_.threadName).mkString(", ")}"
      )
    } yield ()

  private def getCalcMeta(reqCalc: ReqCalc): ZIO[jdbcSession,Throwable,ViewQueryMeta] = for {
    oraSession <- ZIO.service[jdbcSession]
    ora <- oraSession.sess
    _ <- ZIO.logInfo(s"getCalcMeta Oracle SID = ${ora.getPid}")
    vqMeta <- ora.getVqMeta(reqCalc.view_query_id)
  } yield vqMeta

  /**
   * 1. get meta data about this calculation from oracle database.
   * 2.
  */
  def startCalculation(reqCalc: ReqCalc): ZIO[jdbcSession with jdbcChSession, Throwable, Unit] = for {
    _ <- ZIO.logInfo("startCalculation.............................")
/*    oraSession <- ZIO.service[jdbcSession]
    ora <- oraSession.sess*/
    meta <- getCalcMeta(reqCalc)
    _ <- ZIO.logInfo("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    _ <- ZIO.logInfo(meta.query.getOrElse(" -no query- "))
    _ <- ZIO.logInfo("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    _ <- ZIO.logInfo(s"ch_table = ${meta.chTable} params_count = ${meta.params.size}")
  } yield ()

  def copyDataChOra(reqCalc: ReqCalc): ZIO[Any/*jdbcSession with jdbcChSession*/, Throwable, Unit] = for {
    _ <- ZIO.logInfo("copyDataChOra.............................")
    _ <- ZIO.logInfo(s"copyDataChOra for ${reqCalc.view_query_id}")
  } yield ()

}
