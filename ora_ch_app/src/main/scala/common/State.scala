package common

import table.Table

sealed trait StateEnum
case object Ready       extends StateEnum {
  override def toString: String = "Ready"
}
case object Executing   extends StateEnum {
  override def toString: String = "Executing"
}
case object Wait        extends StateEnum {
  override def toString: String = "Wait"
}
case object Calculation extends StateEnum {
  override def toString: String = "Calculation"
}
case object Copying     extends StateEnum {
  override def toString: String = "Copying"
}

case class TaskState(state: StateEnum, currentTable: Option[Table] = Option.empty[Table])

case class CalcState(state: StateEnum)
