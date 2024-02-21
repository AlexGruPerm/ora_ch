package calc

import common.{ CalcState, Wait }
import conf.{ ClickhouseServer, OraServer }
import request.Servers
import zio.json.{ DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder }

case class CalcParams(name: String, value: String)

case class ReqCalcSrc(servers: Servers, view_query_id: Int, params: Set[CalcParams])

case class ReqCalc(
  id: Int = 0,
  id_vq: Int = 0,
  state: CalcState = CalcState(Wait),
  oraServer: Option[OraServer] = Option.empty[OraServer],
  clickhouseServer: Option[ClickhouseServer] = Option.empty[ClickhouseServer],
  params: Set[CalcParams] = Set.empty[CalcParams]
)

import request.EncDecReqNewTaskImplicits._

object EncDecReqCalcImplicits {
  implicit val encoderCalcParams: JsonEncoder[CalcParams] = DeriveJsonEncoder.gen[CalcParams]
  implicit val decoderCalcParams: JsonDecoder[CalcParams] = DeriveJsonDecoder.gen[CalcParams]

  implicit val encoderReqCalc: JsonEncoder[ReqCalcSrc] = DeriveJsonEncoder.gen[ReqCalcSrc]
  implicit val decoderReqCalc: JsonDecoder[ReqCalcSrc] = DeriveJsonDecoder.gen[ReqCalcSrc]
}
