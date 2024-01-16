package table

sealed trait KeyType
//when set pk_columns in input json
object ExtPrimaryKey extends KeyType{
  override def toString: String = "ExtPrimaryKey"
}
object PrimaryKey extends KeyType{
  override def toString: String = "PrimaryKey"
}
object UniqueKey  extends KeyType{
  override def toString: String = "UniqueKey"
}
object RnKey      extends KeyType{
  override def toString: String = "RnKey"
}
