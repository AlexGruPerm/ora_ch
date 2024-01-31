package request

import common.Types.OptStrint
import conf.{ClickhouseServer, OraServer}
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class OneTable(recreate: Int = 1,
                    name: String,
                    plsql_context_date:  OptStrint = Option.empty[String],
                    pk_columns:          OptStrint = Option.empty[String],
                    only_columns:        OptStrint = Option.empty[String],
                    ins_select_order_by: OptStrint = Option.empty[String],
                    partition_by:        OptStrint = Option.empty[String],
                    notnull_columns:     OptStrint = Option.empty[String],
                    where_filter:        OptStrint = Option.empty[String],
                    sync_by_column_max:  OptStrint = Option.empty[String],
                    update_fields:       OptStrint = Option.empty[String]
                   )

case class SrcTable(schema: String, tables: List[OneTable] = List.empty[OneTable])

case class Servers(oracle: OraServer,
                   clickhouse: ClickhouseServer
                  )

case class ReqNewTask(servers: Servers, schemas: List[SrcTable] = List.empty[SrcTable])

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