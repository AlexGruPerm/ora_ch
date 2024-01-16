package request

import conf.{ClickhouseServer, Mode, ModeType, OraServer, Parallel, Sequentially}
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class OneTable(name: String,
                    plsql_context_date: Option[String] = Option.empty[String],
                    pk_columns: Option[String] = Option.empty[String],
                    ins_select_order_by: Option[String] = Option.empty[String],
                    partition_by: Option[String] = Option.empty[String],
                    notnull_columns: Option[List[String]] = Option.empty[List[String]]
                   )

case class SrcTable(schema: String, tables: List[OneTable] = List.empty[OneTable])

case class Servers(oracle: OraServer,
                   clickhouse: ClickhouseServer,
                   config : Mode = Mode()
                  )

case class ReqNewTask(servers: Servers, schemas: List[SrcTable] = List.empty[SrcTable])

object EncDecReqNewTaskImplicits{

  implicit val encoderOneTable: JsonEncoder[OneTable] = DeriveJsonEncoder.gen[OneTable]
  implicit val decoderOneTable: JsonDecoder[OneTable] = DeriveJsonDecoder.gen[OneTable]

  implicit val encoderSrcTable: JsonEncoder[SrcTable] = DeriveJsonEncoder.gen[SrcTable]
  implicit val decoderSrcTable: JsonDecoder[SrcTable] = DeriveJsonDecoder.gen[SrcTable]

  implicit val encoderModeType: JsonEncoder[ModeType] = DeriveJsonEncoder.gen[ModeType]
  implicit val decoderModeType: JsonDecoder[ModeType] = JsonDecoder[String].map {
    case "sequentially" => Sequentially
    case "parallel" => Parallel
    case anyValue => throw new Exception(s"Invalid value in field ret_type = $anyValue")
  }

  implicit val encoderRetType: JsonEncoder[Mode] = DeriveJsonEncoder.gen[Mode]
  implicit val decoderRetType: JsonDecoder[Mode] = DeriveJsonDecoder.gen[Mode]

  implicit val encoderClickhouseServer: JsonEncoder[ClickhouseServer] = DeriveJsonEncoder.gen[ClickhouseServer]
  implicit val decoderClickhouseServer: JsonDecoder[ClickhouseServer] = DeriveJsonDecoder.gen[ClickhouseServer]

  implicit val encoderOraServer: JsonEncoder[OraServer] = DeriveJsonEncoder.gen[OraServer]
  implicit val decoderOraServer: JsonDecoder[OraServer] = DeriveJsonDecoder.gen[OraServer]

  implicit val encoderServers: JsonEncoder[Servers] = DeriveJsonEncoder.gen[Servers]
  implicit val decoderServers: JsonDecoder[Servers] = DeriveJsonDecoder.gen[Servers]

  implicit val encoderReqNewTask: JsonEncoder[ReqNewTask] = DeriveJsonEncoder.gen[ReqNewTask]
  implicit val decoderReqNewTask: JsonDecoder[ReqNewTask] = DeriveJsonDecoder.gen[ReqNewTask]

}