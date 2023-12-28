package task

import task.types.TaskId
import zio.{Ref, Task, ZLayer}

import scala.collection.mutable

object types {
  type TaskId = Int
}

trait TaskRepo {
  def create(task: WsTask): Task[TaskId]
}

case class ImplTaskRepo(ref: Ref[WsTask]) extends TaskRepo {

  def create(task: WsTask): Task[TaskId] = for {
    _ <- ref.update(_ => task)
  } yield task.id

}

object ImplTaskRepo {
  def layer: ZLayer[Any, Nothing, ImplTaskRepo] =
    ZLayer.fromZIO(
      Ref.make(WsTask()).map(r => ImplTaskRepo(r))
    )
}