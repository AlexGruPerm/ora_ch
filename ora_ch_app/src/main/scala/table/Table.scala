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
                 sync_by_column_max: Option[String]
                ){
  def fullTableName(): String =
    s"$schema.$name"
}