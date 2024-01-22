package calc

import request.Servers
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class CalcParams(name: String,
                      value: String)

case class ReqCalc(servers: Servers,
                   view_query_id: Int,
                   params: Set[CalcParams]
                  )

import request.EncDecReqNewTaskImplicits._

object EncDecReqCalcImplicits{
  implicit val encoderCalcParams: JsonEncoder[CalcParams] = DeriveJsonEncoder.gen[CalcParams]
  implicit val decoderCalcParams: JsonDecoder[CalcParams] = DeriveJsonDecoder.gen[CalcParams]

  implicit val encoderReqCalc: JsonEncoder[ReqCalc] = DeriveJsonEncoder.gen[ReqCalc]
  implicit val decoderReqCalc: JsonDecoder[ReqCalc] = DeriveJsonDecoder.gen[ReqCalc]
}
