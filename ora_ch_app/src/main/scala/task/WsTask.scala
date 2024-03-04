package task

import common.{TaskState, Wait}
import conf.{ClickhouseServer, OraServer}
import request.Parallel
import table.Table

case class WsTask(
  id: Int = 0,
  state: TaskState = TaskState(Wait, Option.empty[Table]),
  oraServer: Option[OraServer] = Option.empty[OraServer],
  clickhouseServer: Option[ClickhouseServer] = Option.empty[ClickhouseServer],
  parallel: Parallel = Parallel(),
  tables: List[Table] = List.empty[Table],
  parDegree: Int = 2
)
