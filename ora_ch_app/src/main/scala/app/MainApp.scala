package app

import server.WServer
import task.ImplTaskRepo
import zio.{ZIO, _}
import zio.http._

object MainApp extends ZIOAppDefault {

  def app = ZIO.withLogger(ZLogger.default.map(println(_)).filterLogLevel(_ >= LogLevel.Info)) {
      (Server.install(WServer.app).flatMap { port =>
        ZIO.logInfo(s"Started server on port: $port")
      } *> ZIO.never)
        .provide(ImplTaskRepo.layer,
          Server.defaultWithPort(8081))
  }.provide(Runtime.removeDefaultLoggers)

  override def run: ZIO[Any,Throwable,Unit] =
    for {
      _ <- app
    } yield ()

  /*
  def app = ZIO.withLogger(ZLogger.default.map(println(_)).filterLogLevel(_ >= LogLevel.Debug)) {
     for {
       _ <- ZIO.log("log1")
       _ <- ZIO.logInfo("logInfo")
       _ <- ZIO.logDebug("logDebug")
       _ <- ZIO.logError("logError")
       _ <- ZIO.log("log2")
     } yield ()
   }.provide(Runtime.removeDefaultLoggers)
  */

}