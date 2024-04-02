package common

sealed trait UpdateTableType

case object LeftJoin extends UpdateTableType {
  override def toString: String = "LeftJoin"
}

case object MergeTree extends UpdateTableType {
  override def toString: String = "MergeTree"
}
