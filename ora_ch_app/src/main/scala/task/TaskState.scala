package task

import table.Table

sealed trait TaskStateEnum
object Wait extends TaskStateEnum
object Ready extends TaskStateEnum
object Executing extends TaskStateEnum


case class TaskState(state: TaskStateEnum,currentTable :Option[Table])
