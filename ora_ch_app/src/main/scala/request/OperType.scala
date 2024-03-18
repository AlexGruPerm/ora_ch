package request

sealed trait OperType{
  val operStr: String
  def getRecreate: Int = 1
}
/**
 * Drop table in ch and create new with data from ora.
*/
case object Recreate extends OperType{
  override val operStr: String = "recreate"
  override def toString: String = s"operation = $operStr"
}
/**
 * Delete rows and append new rows from ora to ch:
 * 1)
 *  delete from ch.table where "where_filter"
 * 2)
 * insert into ch.table
 *  select * from ora.table where "where_filter"
 *
 *  p.s. Step 1 needed to eliminate possible duplicates.
*/
case object AppendWhere extends OperType{
  override val operStr: String = "append_where"
  override def getRecreate: Int = 0
  override def toString: String = s"operation = $operStr"
}

/**
 * Only append new rows in ch which not exists (without delete).
 *  insert into ch.table
 *    select * from ora.table where (c1,c2,...) not in (select distinct c1,c2,... from ch.table)
*/
case object AppendNotIn extends OperType{
  override val operStr: String = "append_notin"
  override def getRecreate: Int = 0
  override def toString: String = s"operation = $operStr"
}

/**
 * Only append new rows in ch which not exists (without delete).
 *  insert into ch.table
 *    select * from ora.table where single_column > (select max(single_column) from ch.table)
*/
case object AppendByMax extends OperType{
  override val operStr: String = "append_bymax"
  override def getRecreate: Int = 0
  override def toString: String = s"operation = $operStr"
}

/**
 * In this case only used PRIMARY KEY FROM Clickhouse, look at sessCh.getPkColumns
 * and update_fields from json.
*/
case object Update extends OperType{
  override val operStr: String = "update"
  override def getRecreate: Int = 0
  override def toString: String = s"operation = $operStr"
}
