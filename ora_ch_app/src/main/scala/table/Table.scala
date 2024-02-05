package table

import common.Types.OptString

/**
 * Table or view
 */
case class Table(schema: String,
                 recreate: Int = 0,
                 name: String,
                 keyType: KeyType,
                 keyColumns: String,
                 plsql_context_date:  OptString,
                 pk_columns:          OptString,
                 only_columns:        OptString,
                 ins_select_order_by: OptString,
                 partition_by:        OptString,
                 notnull_columns:     OptString,
                 where_filter:        OptString,
                 sync_by_column_max:  OptString,
                 update_fields:       OptString,
                 sync_by_columns:     OptString
                ){

  println(s"Table constr: $pk_columns - $only_columns - $notnull_columns")

  def syncArity(): Int =
    sync_by_columns match {
      case Some(syncFields) => syncFields.split(",").length
      case None => 0
    }

  def fullTableName(): String =
    s"$schema.$name"

  def updateColumns(): String =
    update_fields.getOrElse("empty_update_fields")

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

  def finishStatus(): String =
    sync_by_column_max match {
      case Some(_) => "finished_append"
      case None =>
        update_fields match {
        case Some(_) => "finished_update"
        case None => "finished"
      }
    }

}