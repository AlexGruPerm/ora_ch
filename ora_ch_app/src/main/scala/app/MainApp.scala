package app

import server.WServer
import task.ImplTaskRepo
import zio.{ZIO, _}
import zio.http._

object MainApp extends ZIOAppDefault {

  override def run: ZIO[Any,Throwable,Nothing] =
    (Server.install(WServer.app).flatMap { port =>
      Console.printLine(s"Started server on port: $port")
    } *> ZIO.never)
      .provide(ImplTaskRepo.layer,
        Server.defaultWithPort(8081))

  /*
  val listEffects: List[ZIO[Any,Nothing,Int]] = List(ZIO.succeed(1),ZIO.succeed(1),ZIO.succeed(1))
  ZIO.collectAllPar(listEffects).withParallelism(3)
  */

}