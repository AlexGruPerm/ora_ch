package app

import server.WServer
import zio._
import zio.http._

object MainApp extends ZIOAppDefault {

  override def run: ZIO[Any,Throwable,Nothing] =
    (Server.install(WServer.app).flatMap { port =>
      Console.printLine(s"Started server on port: $port")
    } *> ZIO.never).provide(Server.defaultWithPort(8081))

}