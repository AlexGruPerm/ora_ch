package table

/**
 * Table or view
 */
case class Table(schema: String,
                 recreate: Int = 0,
                 name: String,
                 keyType: KeyType,
                 keyColumns: String,
                 plsql_context_date: Option[String],
                 pk_columns: Option[String],
                 only_columns: Option[List[String]],
                 ins_select_order_by: Option[String],
                 partition_by: Option[String],
                 notnull_columns: Option[List[String]],
                 where_filter: Option[String],
                 sync_by_column_max: Option[String],
                 update_fields: Option[List[String]]
                ){
  def fullTableName(): String =
    s"$schema.$name"

  def updateColumns(): String =
    update_fields.getOrElse(List("empty_update_fields")).mkString(",")

  def whereFilter(): String =
    where_filter match {
      case Some(filter) => s" where $filter"
      case None => " "
    }

  def orderBy(): String =
    ins_select_order_by match {
      case Some(order) => s" order by $order"
      case None => " "
    }

}