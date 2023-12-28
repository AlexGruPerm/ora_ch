package table

sealed trait KeyType
object PrimaryKey extends KeyType
object UniqueKey  extends KeyType
object RnKey      extends KeyType
