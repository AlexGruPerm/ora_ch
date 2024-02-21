package task

import common.{ TaskState, Wait }
import conf.{ ClickhouseServer, OraServer }
import table.Table

case class WsTask(
  id: Int = 0,
  state: TaskState = TaskState(Wait, Option.empty[Table]),
  oraServer: Option[OraServer] = Option.empty[OraServer],
  clickhouseServer: Option[ClickhouseServer] = Option.empty[ClickhouseServer],
  tables: List[Table] = List.empty[Table]
)
