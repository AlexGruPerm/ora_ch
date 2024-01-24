package calc

import calc.types.CalcId
import zio.{Ref, Task, UIO, ZIO, ZLayer}
import common._

object types {
  type CalcId = Int
}

trait CalcRepo {
  def create(task: ReqCalc): Task[CalcId]
  def getCalcId: UIO[Int]
  def setState(newState: CalcState): UIO[Unit]
  def getState: UIO[CalcState]
  def clearCalc: UIO[Unit]
}

case class ImplCalcRepo(ref: Ref[ReqCalc]) extends CalcRepo {

  def create(task: ReqCalc): Task[CalcId] = for {
    currStatus <- getState
    _ <- ref.update(_ => task)
    _ <- ZIO.logInfo(s"repo state changed: ${currStatus.state} -> ${task.state}")
  } yield task.id

  def getCalcId: UIO[Int] = ref.get.map(_.id)

  def setState(newState: CalcState): UIO[Unit] = for {
    currStatus <- getState
    _ <- ref.update(wst => wst.copy(state = newState))
    _ <- ZIO.logInfo(s"repo state changed: ${currStatus.state} -> ${newState.state}")
  } yield ()

  def getState: UIO[CalcState] =
    ref.get.map(_.state)

  def clearCalc: UIO[Unit] = ref.update(_ => ReqCalc())

}

object ImplCalcRepo {
  def layer: ZLayer[Any, Nothing, ImplCalcRepo] =
    ZLayer.fromZIO(
      Ref.make(ReqCalc()).map(r => ImplCalcRepo(r))
    )
}


