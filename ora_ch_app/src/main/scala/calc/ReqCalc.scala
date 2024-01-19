package calc

import request.Servers
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class CalcParams(
                      date_cache_1: Int,
                      datecalc_cache_1: Int,
                      date_calc_ctr: String,
                      year_bo: Int,
                      c_year: Int
                     )

case class ReqCalc(servers: Servers,
                   view_query_id: Int,
                   params: CalcParams
                  )

import request.EncDecReqNewTaskImplicits._

object EncDecReqCalcImplicits{
  implicit val encoderCalcParams: JsonEncoder[CalcParams] = DeriveJsonEncoder.gen[CalcParams]
  implicit val decoderCalcParams: JsonDecoder[CalcParams] = DeriveJsonDecoder.gen[CalcParams]

  implicit val encoderReqCalc: JsonEncoder[ReqCalc] = DeriveJsonEncoder.gen[ReqCalc]
  implicit val decoderReqCalc: JsonDecoder[ReqCalc] = DeriveJsonDecoder.gen[ReqCalc]
}
