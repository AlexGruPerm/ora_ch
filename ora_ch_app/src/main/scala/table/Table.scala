package table

import common.Types.{ MaxValAndCnt, OptString }
import request.{ AppendByMax, AppendNotIn, AppendWhere, OperType, Recreate, Update }

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
  // pk_columns: String,
  only_columns: OptString,
  // ins_select_order_by: OptString,
  // partition_by: OptString,
  // notnull_columns: OptString,
  where_filter: OptString,
  sync_by_column_max: OptString,
  update_fields: OptString,
  sync_by_columns: OptString,
  sync_update_by_column_max: OptString,
  clr_ora_table_aft_upd: OptString,
  order_by_ora_data: OptString,
  src_table_full_name: OptString
) {
  // println(s"Table constr: $pk_columns - $only_columns - $notnull_columns")

  def syncArity(): Int =
    sync_by_columns match {
      case Some(syncFields) => syncFields.split(",").length
      case None             => 0
    }

  def fullTableName(): String =
    s"$schema.$name"

  def updateColumns(): String =
    update_fields.getOrElse("empty_update_fields")

  // case Recreate | AppendWhere | AppendByMax | AppendNotIn =>
  def whereFilter(maxValCnt: Option[MaxValAndCnt], appendKeys: Option[List[Any]]): String =
    operation match {
      case Recreate | AppendWhere | AppendByMax =>
        (where_filter, maxValCnt) match {
          case (Some(filter), Some(sync)) =>
            if (sync.MaxValue > 0)
              s" where $filter and ${sync_by_column_max.getOrElse("xxx")} > ${sync.MaxValue}  "
            else
              s" where $filter "
          case (Some(filter), None)       => s" where $filter "
          case (None, Some(sync))         =>
            s" where ${sync_by_column_max.getOrElse("xxx")} > ${sync.MaxValue}  "
          case _                          => s" "
        }
      case AppendNotIn                          =>
        val filterStr           = s" ( ${sync_by_columns.getOrElse(" EMPTY_SYNC_BY_COLUMNS ")} ) "
        val nin_Filters: String = appendKeys match {
          case Some(appendKeysList) =>
            if (syncArity() == 1) {
              s"(${appendKeysList.mkString(",")})"
            } else if (syncArity() == 2) {
              s"""(${appendKeysList
                  .map(lst =>
                    s"(${lst.asInstanceOf[(Int, Int)].productIterator.toList.mkString(",")})"
                  )
                  .mkString(",")})"""
            } else if (syncArity() == 3) {
              s"""(${appendKeysList
                  .map(lst =>
                    s"(${lst.asInstanceOf[(Int, Int, Int)].productIterator.toList.mkString(",")})"
                  )
                  .mkString(",")})"""
            } else
              " "
          case None                 => " "
        }
        if (appendKeys.nonEmpty)
          s" where $filterStr not in $nin_Filters "
        else
          " "
      case Update                               => " "
    }

  def orderBy(): String =
    order_by_ora_data match {
      case Some(order) => s" order by $order"
      case None        => " "
    }

  def finishStatus(): String =
    s"finished_${operation.operStr}"

}
