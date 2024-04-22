package calc

case class VQParams(
  name: String,
  chType: String,
  ord: Int
)

case class ViewQueryMeta(
  chTable: String,
  oraTable: String,
  query: Option[String],
  params: Set[VQParams],
  chSchema: String,
  oraSchema: String,
  copyChOraColumns: String
)
