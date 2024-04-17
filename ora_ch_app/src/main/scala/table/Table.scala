package table

import common.Types.OptString
import request.OperType

/**
 * Table or view
 */
case class Table(
  schema: String,
  recreate: Int = 0,
  operation: OperType,
  name: String,
  curr_date_context: OptString,
  analyt_datecalc: OptString,
  //pk_columns: String,
  only_columns: OptString,
  //ins_select_order_by: OptString,
  //partition_by: OptString,
  //notnull_columns: OptString,
  where_filter: OptString,
  sync_by_column_max: OptString,
  update_fields: OptString,
  sync_by_columns: OptString,
  sync_update_by_column_max: OptString,
  clr_ora_table_aft_upd: OptString,
  order_by_ora_data: OptString
) {
  //println(s"Table constr: $pk_columns - $only_columns - $notnull_columns")

  def syncArity(): Int =
    sync_by_columns match {
      case Some(syncFields) => syncFields.split(",").length
      case None             => 0
    }

  def fullTableName(): String =
    s"$schema.$name"

  def updateColumns(): String =
    update_fields.getOrElse("empty_update_fields")

  def whereFilter(): String =
    where_filter match {
      case Some(filter) => s" where $filter"
      case None         => " "
    }

  def orderBy(): String =
    order_by_ora_data match {
      case Some(order) => s" order by $order"
      case None        => " "
    }

  def finishStatus(): String =
    s"finished_${operation.operStr}"

}
