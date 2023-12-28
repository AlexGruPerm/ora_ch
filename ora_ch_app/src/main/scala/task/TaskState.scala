package task

import table.Table

sealed trait TaskStateEnum
case object Wait extends TaskStateEnum{
  override def toString: String = "Wait"
}
case object Ready extends TaskStateEnum{
  override def toString: String = "Ready"
}
case object Executing extends TaskStateEnum{
  override def toString: String = "Executing"
}

case class TaskState(state: TaskStateEnum,currentTable :Option[Table] = Option.empty[Table])
