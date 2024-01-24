package app

import calc.ImplCalcRepo
import server.WServer
import task.ImplTaskRepo
import zio.{ZIO, _}
import zio.http._

object MainApp extends ZIOAppDefault {

  def app: ZIO[Any,Throwable,Nothing] = ZIO.withLogger(ZLogger.default.map(println(_)).filterLogLevel(_ >= LogLevel.Info)) {
      (Server.install(WServer.app).flatMap { port =>
        ZIO.logInfo(s"Started server on port: $port")
      } *> ZIO.never)
        .provide(ImplTaskRepo.layer,
          ImplCalcRepo.layer,
          Server.defaultWithPort(8081))
  }.provide(Runtime.removeDefaultLoggers)

  override def run: ZIO[Any,Throwable,Unit] =
    for {
      _ <- app
    } yield ()

}