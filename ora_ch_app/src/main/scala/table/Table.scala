package table

/**
 * Table or view
 */
case class Table(schema: String,
                 name: String,
                 keyType: KeyType,
                 keyColumns: String,
                 plsql_context_date: Option[String],
                 pk_columns: Option[String],
                 ins_select_order_by: Option[String],
                 partition_by: Option[String],
                 notnull_columns: Option[List[String]]
         /*      skipColumns: List[String] = List.empty[String],
                 fetch_size: Option[Int],
                 batch_size: Option[Int],
                 rowsProcessed: Int = 0*/
                )