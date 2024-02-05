package common

object Types {

  type OptString = Option[String]

  case class MaxValAndCnt(MaxValue: Long, CntRows: Long)

}
