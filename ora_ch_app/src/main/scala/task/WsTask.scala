package task

import conf.{ClickhouseServer, Mode, OraServer}
import table.Table

case class WsTask(
                  id :Int = 0,
                  state: TaskState = TaskState(Wait,Option.empty[Table]),
                  oraServer: Option[OraServer] = Option.empty[OraServer],
                  clickhouseServer: Option[ClickhouseServer] = Option.empty[ClickhouseServer],
                  mode : Mode = Mode(),
                  plsql_context_date: Option[String] = Option.empty[String],
                  tables: List[Table] = List.empty[Table]
                 )


