package table

import java.time.OffsetDateTime

case class TableStatus(beginDatetime: OffsetDateTime,
                       endDatetime: OffsetDateTime,
                       durationMsec: Long,
                       rows: Int = 0,
                       error: Option[TableError])