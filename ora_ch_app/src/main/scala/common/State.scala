package common

import table.Table

sealed trait StateEnum
case object Wait extends StateEnum {
  override def toString: String = "Wait"
}
case object Ready extends StateEnum {
  override def toString: String = "Ready"
}
case object Executing extends StateEnum {
  override def toString: String = "Executing"
}

case class TaskState(state: StateEnum,currentTable :Option[Table] = Option.empty[Table])

case class CalcState(state: StateEnum)
