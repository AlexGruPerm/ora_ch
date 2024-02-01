package table

import common.Types.OptStrint

/**
 * Table or view
 */
case class Table(schema: String,
                 recreate: Int = 0,
                 name: String,
                 keyType: KeyType,
                 keyColumns: String,
                 plsql_context_date:  OptStrint,
                 pk_columns:          OptStrint,
                 only_columns:        OptStrint,
                 ins_select_order_by: OptStrint,
                 partition_by:        OptStrint,
                 notnull_columns:     OptStrint,
                 where_filter:        OptStrint,
                 sync_by_column_max:  OptStrint,
                 update_fields:       OptStrint
                ){

  println(s"Table constr: $pk_columns - $only_columns - $notnull_columns")

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