package task

import task.types.TaskId
import zio.{Ref, Task, UIO, ZLayer}

import scala.collection.mutable

object types {
  type TaskId = Int
}

trait TaskRepo {
  def create(task: WsTask): Task[TaskId]
  def getTaskId: UIO[Int]
  def setState(newState: TaskState): UIO[Unit]
  def getState: UIO[TaskState]
}

case class ImplTaskRepo(ref: Ref[WsTask]) extends TaskRepo {

  def create(task: WsTask): Task[TaskId] = for {
    _ <- ref.update(_ => task)
  } yield task.id

  def getTaskId: UIO[Int] = ref.get.map(_.id)

  def setState(newState: TaskState): UIO[Unit] =
    ref.update(wst => wst.copy(state = newState))

  def getState: UIO[TaskState] =
    ref.get.map(_.state)

}

object ImplTaskRepo {
  def layer: ZLayer[Any, Nothing, ImplTaskRepo] =
    ZLayer.fromZIO(
      Ref.make(WsTask()).map(r => ImplTaskRepo(r))
    )
}