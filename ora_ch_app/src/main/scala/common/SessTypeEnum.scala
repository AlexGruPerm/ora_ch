package common

sealed trait SessTypeEnum
case object SessTask extends SessTypeEnum {
  override def toString: String = "Task"
}
case object SessCalc extends SessTypeEnum {
  override def toString: String = "Calc"
}
