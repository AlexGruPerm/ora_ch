package request

import common.Types.OptString
import conf.{ClickhouseServer, OraServer}
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class OneTable(recreate: Int = 1,
                    name: String,
                    curr_date_context:         OptString = Option.empty[String],
                    analyt_datecalc:           OptString = Option.empty[String],
                    pk_columns:                String,
                    only_columns:              OptString = Option.empty[String],
                    ins_select_order_by:       OptString = Option.empty[String],
                    partition_by:              OptString = Option.empty[String],
                    notnull_columns:           OptString = Option.empty[String],
                    where_filter:              OptString = Option.empty[String],
                    sync_by_column_max:        OptString = Option.empty[String],
                    update_fields:             OptString = Option.empty[String],
                    sync_by_columns:           OptString = Option.empty[String],
                    sync_update_by_column_max: OptString = Option.empty[String]
                   ){
  if (recreate == 1 && sync_update_by_column_max.nonEmpty)
    throw new Exception("recreate = 1 incompatible with non empty sync_update_by_column_max.")

  if (sync_update_by_column_max.nonEmpty && update_fields.isEmpty)
    throw new Exception("Non empty sync_update_by_column_max incompatible with empty update_fields.")

  if (recreate == 1 && sync_by_columns.nonEmpty)
    throw new Exception("recreate = 1 incompatible with non empty sync_by_column_max.")

  if (recreate == 1 && sync_by_columns.nonEmpty)
    throw new Exception("recreate = 1 incompatible with non empty sync_by_columns.")

  if (recreate == 1 && update_fields.nonEmpty)
    throw new Exception("recreate = 1 incompatible with non empty update_fields.")

  if (sync_by_columns.getOrElse("").split(",").length > 3)
    throw new Exception("sync_by_columns supports only up to three fields with Int type.")

  if (sync_by_columns.nonEmpty && sync_by_column_max.nonEmpty)
    throw new Exception("not empty sync_by_column_max incompatible with non empty sync_by_columns.")

}

case class SrcTable(schema: String, tables: List[OneTable])

case class Servers(oracle: OraServer, clickhouse: ClickhouseServer)

case class ReqNewTask(servers: Servers, schemas: List[SrcTable] = List.empty[SrcTable]){
  if (schemas.exists(st => st.tables.isEmpty))
    throw new Exception(s"tables array is empty for schema ${schemas.find(st => st.tables.isEmpty).map(s => s.schema)}")
}

object EncDecReqNewTaskImplicits{

  implicit val encoderOneTable: JsonEncoder[OneTable] = DeriveJsonEncoder.gen[OneTable]
  implicit val decoderOneTable: JsonDecoder[OneTable] = DeriveJsonDecoder.gen[OneTable]

  implicit val encoderSrcTable: JsonEncoder[SrcTable] = DeriveJsonEncoder.gen[SrcTable]
  implicit val decoderSrcTable: JsonDecoder[SrcTable] = DeriveJsonDecoder.gen[SrcTable]

  implicit val encoderClickhouseServer: JsonEncoder[ClickhouseServer] = DeriveJsonEncoder.gen[ClickhouseServer]
  implicit val decoderClickhouseServer: JsonDecoder[ClickhouseServer] = DeriveJsonDecoder.gen[ClickhouseServer]

  implicit val encoderOraServer: JsonEncoder[OraServer] = DeriveJsonEncoder.gen[OraServer]
  implicit val decoderOraServer: JsonDecoder[OraServer] = DeriveJsonDecoder.gen[OraServer]

  implicit val encoderServers: JsonEncoder[Servers] = DeriveJsonEncoder.gen[Servers]
  implicit val decoderServers: JsonDecoder[Servers] = DeriveJsonDecoder.gen[Servers]

  implicit val encoderReqNewTask: JsonEncoder[ReqNewTask] = DeriveJsonEncoder.gen[ReqNewTask]
  implicit val decoderReqNewTask: JsonDecoder[ReqNewTask] = DeriveJsonDecoder.gen[ReqNewTask]

}