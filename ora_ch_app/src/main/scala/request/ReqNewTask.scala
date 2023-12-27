package request

import conf.{ClickhouseServer, Mode, ModeType, OraServer, Parallel, Sequentially}
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class OneTable(name: String)

case class SrcTable(schema: String, tables: Option[List[OneTable]])

case class Servers(oracle: OraServer,
                   clickhouse: ClickhouseServer,
                   config : Mode = Mode()
                  )

case class ReqNewTask(servers: Servers, schemas: Option[List[SrcTable]])

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

/*
package request

import conf.{ClickhouseServer, Mode, ModeType, OraServer, Parallel, Sequentially}
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class SrcTable(schema: String, name: String)

case class Servers(oracle: OraServer,
                   clickhouse: ClickhouseServer,
                   config : Mode = Mode()
                  )

case class ReqNewTask(servers: Servers,tables: Option[List[SrcTable]])

object EncDecReqNewTaskImplicits{

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
*/