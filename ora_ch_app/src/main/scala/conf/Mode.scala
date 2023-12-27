package conf

sealed trait ModeType
case object  Sequentially extends ModeType{
  override def toString: String = "sequentially"
}
case object  Parallel extends ModeType{
  override def toString: String = "parallel"
}

case class Mode(mode: ModeType = Sequentially, degree : Int = 1)

