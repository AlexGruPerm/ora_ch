package table

/**
 * Table or view
 */
case class Table(schema: String,
                 name: String,
                 keyType: KeyType,
                 keyColumns: String
         /*      skipColumns: List[String] = List.empty[String],
                 fetch_size: Option[Int],
                 batch_size: Option[Int],
                 rowsProcessed: Int = 0*/
                )