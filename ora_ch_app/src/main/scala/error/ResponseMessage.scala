package error

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class ResponseMessage(message: String)

object ResponseMessage{
  implicit val encoderInputJsonParsingError: JsonEncoder[ResponseMessage] = DeriveJsonEncoder.gen[ResponseMessage]
  implicit val decoderInputJsonParsingError: JsonDecoder[ResponseMessage] = DeriveJsonDecoder.gen[ResponseMessage]
}
