package app

import calc.ImplCalcRepo
import server.WServer
import task.ImplTaskRepo
import zio.{ ZIO, _ }
import zio.http._

object MainApp extends ZIOAppDefault {

    val logger: ZLogger[String, Unit] =
    new ZLogger[String, Unit] {
      override def apply(
                          trace: Trace,
                          fiberId: FiberId,
                          logLevel: LogLevel,
                          message: () => String,
                          cause: Cause[Any],
                          context: FiberRefs,
                          spans: List[LogSpan],
                          annotations: Map[String, String]
                        ): Unit =
        println(s"${java.time.Instant.now()} - ${logLevel.label} - ${message()}")
    }

    def app: ZIO[Any, Throwable, Nothing] = ZIO.withLogger(logger.filterLogLevel(_ >= LogLevel.Info)) {
      (Server.install(WServer.app(60)).flatMap { port =>
        ZIO.logInfo(s"Started server on port: $port")
      } *> ZIO.never)
        .provide(ImplCalcRepo.layer, ImplTaskRepo.layer, Server.defaultWithPort(8081))
    }
    .provide(Runtime.removeDefaultLoggers)

  override def run: ZIO[Any, Throwable, Unit] =
    for {
      _ <- app
    } yield ()

}
