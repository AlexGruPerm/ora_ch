package request

import common.Types.OptString
import conf.{ ClickhouseServer, OraServer }
import zio.json.{ DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder }

case class OneTable(
  operation: OperType,
  name: String,
  curr_date_context: OptString = Option.empty[String],
  analyt_datecalc: OptString = Option.empty[String],
  // pk_columns: String,
  only_columns: OptString = Option.empty[String],
  // ins_select_order_by: OptString = Option.empty[String],
  // partition_by: OptString = Option.empty[String],
  // notnull_columns: OptString = Option.empty[String],
  where_filter: OptString = Option.empty[String],
  sync_by_column_max: OptString = Option.empty[String],
  update_fields: OptString = Option.empty[String],
  sync_by_columns: OptString = Option.empty[String],
  sync_update_by_column_max: OptString = Option.empty[String],
  clr_ora_table_aft_upd: OptString = Option.empty[String],
  order_by_ora_data: OptString = Option.empty[String]
) {
  // println(s"... DEBUG [OneTable] $operation where_filter.isEmpty=${where_filter.isEmpty}")

  if (operation != Update && clr_ora_table_aft_upd.nonEmpty)
    throw new Exception(s"$operation incompatible with non empty clr_ora_table_aft_upd.")

  if (operation == AppendNotIn && sync_by_columns.isEmpty)
    throw new Exception(s"$operation incompatible with empty sync_by_columns.")

  if (operation == AppendWhere && where_filter.isEmpty)
    throw new Exception(s"$operation incompatible with empty where_filter.")

  if (operation == AppendByMax && sync_by_column_max.isEmpty)
    throw new Exception(s"$operation incompatible with empty sync_by_column_max.")

  if (operation.getRecreate == 1 && sync_update_by_column_max.nonEmpty)
    throw new Exception(s"$operation incompatible with non empty sync_update_by_column_max.")

  if (sync_update_by_column_max.nonEmpty && update_fields.isEmpty)
    throw new Exception(
      "Non empty sync_update_by_column_max incompatible with empty update_fields."
    )

  if (operation.getRecreate == 1 && sync_by_columns.nonEmpty)
    throw new Exception(s"$operation incompatible with non empty sync_by_column_max.")

  if (operation.getRecreate == 1 && sync_by_columns.nonEmpty)
    throw new Exception(s"$operation incompatible with non empty sync_by_columns.")

  if (operation.getRecreate == 1 && update_fields.nonEmpty)
    throw new Exception(s"$operation incompatible with non empty update_fields.")

  if (sync_by_columns.getOrElse("").split(",").length > 3)
    throw new Exception("sync_by_columns supports only up to three fields with Int type.")

  if (sync_by_columns.nonEmpty && sync_by_column_max.nonEmpty)
    throw new Exception("not empty sync_by_column_max incompatible with non empty sync_by_columns.")
}

case class SrcTable(schema: String, tables: List[OneTable])

case class Servers(oracle: OraServer, clickhouse: ClickhouseServer)

case class Parallel(degree: Int = 2) {
  def plus1: Parallel = Parallel(degree + 1)
}

case class ReqNewTask(
  servers: Servers,
  parallel: Parallel = Parallel(),
  schemas: List[SrcTable] = List.empty[SrcTable]
) {
  if (schemas.exists(st => st.tables.isEmpty))
    throw new Exception(
      s"tables array is empty for schema ${schemas.find(st => st.tables.isEmpty).map(s => s.schema)}"
    )

  if (parallel.degree < 2 || parallel.degree > 12)
    throw new Exception(
      s"parallel.degree = ${parallel.degree} must be between 2 and 12. Or without this key."
    )

}

object EncDecReqNewTaskImplicits {

  implicit val encoderOperType: JsonEncoder[OperType] = DeriveJsonEncoder.gen[OperType]
  implicit val decoderOperType: JsonDecoder[OperType] = JsonDecoder[String].map {
    case "recreate"     => Recreate
    case "append_where" => AppendWhere
    case "append_notin" => AppendNotIn
    case "append_bymax" => AppendByMax
    case "update"       => Update
    case anyValue       =>
      throw new Exception(s"Invalid value in field operation = $anyValue (decoderOperType)")
  }

  implicit val encoderParallel: JsonEncoder[Parallel] = DeriveJsonEncoder.gen[Parallel]
  implicit val decoderParallel: JsonDecoder[Parallel] = DeriveJsonDecoder.gen[Parallel]

  implicit val encoderOneTable: JsonEncoder[OneTable] = DeriveJsonEncoder.gen[OneTable]
  implicit val decoderOneTable: JsonDecoder[OneTable] = DeriveJsonDecoder.gen[OneTable]

  implicit val encoderSrcTable: JsonEncoder[SrcTable] = DeriveJsonEncoder.gen[SrcTable]
  implicit val decoderSrcTable: JsonDecoder[SrcTable] = DeriveJsonDecoder.gen[SrcTable]

  implicit val encoderClickhouseServer: JsonEncoder[ClickhouseServer] =
    DeriveJsonEncoder.gen[ClickhouseServer]
  implicit val decoderClickhouseServer: JsonDecoder[ClickhouseServer] =
    DeriveJsonDecoder.gen[ClickhouseServer]

  implicit val encoderOraServer: JsonEncoder[OraServer] = DeriveJsonEncoder.gen[OraServer]
  implicit val decoderOraServer: JsonDecoder[OraServer] = DeriveJsonDecoder.gen[OraServer]

  implicit val encoderServers: JsonEncoder[Servers] = DeriveJsonEncoder.gen[Servers]
  implicit val decoderServers: JsonDecoder[Servers] = DeriveJsonDecoder.gen[Servers]

  implicit val encoderReqNewTask: JsonEncoder[ReqNewTask] = DeriveJsonEncoder.gen[ReqNewTask]
  implicit val decoderReqNewTask: JsonDecoder[ReqNewTask] = DeriveJsonDecoder.gen[ReqNewTask]

}
