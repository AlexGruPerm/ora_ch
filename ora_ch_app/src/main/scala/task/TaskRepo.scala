package task

import common.TaskState
import task.types.TaskId
import zio.{Ref, Task, UIO, ZIO, ZLayer}

import scala.collection.mutable

object types {
  type TaskId = Int
}

trait TaskRepo {
  def create(task: WsTask): Task[TaskId]
  def getTaskId: UIO[Int]
  def setState(newState: TaskState): UIO[Unit]
  def getState: UIO[TaskState]
  def setTaskId(taskId: Int): UIO[Unit]
  def clearTask: UIO[Unit]
}

case class ImplTaskRepo(ref: Ref[WsTask]) extends TaskRepo {

  def create(task: WsTask): Task[TaskId] = for {
    currStatus <- getState
    _ <- ref.update(_ => task)
    _ <- ZIO.logDebug(s"repo state changed: ${currStatus.state} -> ${task.state.state}")
  } yield task.id

  def getTaskId: UIO[Int] = ref.get.map(_.id)

  def setState(newState: TaskState): UIO[Unit] = for {
    currStatus <- getState
    _ <- ref.update(wst => wst.copy(state = newState))
    _ <- ZIO.logDebug(s"repo state changed: ${currStatus.state} -> ${newState.state}")
  } yield ()

  def setTaskId(taskId: Int): UIO[Unit] = ref.update(wst => wst.copy(id = taskId))

  def getState: UIO[TaskState] =
    ref.get.map(_.state)

  def clearTask: UIO[Unit] = ref.update(_ => WsTask())

}

object ImplTaskRepo {
  def layer: ZLayer[Any, Nothing, ImplTaskRepo] =
    ZLayer.fromZIO(
      Ref.make(WsTask()).map(r => ImplTaskRepo(r))
    )
}