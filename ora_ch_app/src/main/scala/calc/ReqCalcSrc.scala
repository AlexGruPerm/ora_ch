package calc

import common.{ CalcState, Wait }
import conf.{ ClickhouseServer, OraServer }
import request.Servers
import zio.json.{ DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder }

case class CalcParams(name: String, value: String)

/**
 * order_by set order of calculation in List of queries. If exists != 0 order_by then calculation
 * sequential If all queries have order_by=0 then we can calculate queries in parallel.
 */
case class Query(query_id: Int, order_by: Int, params: Set[CalcParams]) {
  def paramByName(pName: String): String =
    params.find(_.name == pName).getOrElse(CalcParams(pName, "*")).value.trim
}

case class ReqCalcSrc(
  servers: Servers,
  id_reload_calc: Int,
  queries: List[Query] = List.empty[Query]
)

case class ReqCalc(
  id: Int = 0,
  // id_vq: Int = 0,
  state: CalcState = CalcState(Wait) // ,
  // oraServer: Option[OraServer] = Option.empty[OraServer],
  // clickhouseServer: Option[ClickhouseServer] = Option.empty[ClickhouseServer],
  // params: Set[CalcParams] = Set.empty[CalcParams]
)

import request.EncDecReqNewTaskImplicits._

object EncDecReqCalcImplicits {
  implicit val encoderCalcParams: JsonEncoder[CalcParams] = DeriveJsonEncoder.gen[CalcParams]
  implicit val decoderCalcParams: JsonDecoder[CalcParams] = DeriveJsonDecoder.gen[CalcParams]

  implicit val encoderQuery: JsonEncoder[Query] = DeriveJsonEncoder.gen[Query]
  implicit val decoderQuery: JsonDecoder[Query] = DeriveJsonDecoder.gen[Query]

  implicit val encoderReqCalc: JsonEncoder[ReqCalcSrc] = DeriveJsonEncoder.gen[ReqCalcSrc]
  implicit val decoderReqCalc: JsonDecoder[ReqCalcSrc] = DeriveJsonDecoder.gen[ReqCalcSrc]
}
