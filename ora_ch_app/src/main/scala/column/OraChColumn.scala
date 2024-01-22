package column

case class OraChColumn(name: String,
                       typeName: String,
                       typeClass: String,
                       displaySize: Int,
                       precision: Int,
                       scale: Int,
                       isNullable: Int,
                       clColumnString: String)

object OraChColumn {
  /**
   * create table test(
   * c1 integer,
   * c2 number not null,
   * c3 varchar2(2560),
   * c4 date,
   * c5 timestamp
   * );
   * getColumnTypeName  getColumnClassName    getColumnDisplaySize  getPrecision  getScale  isNullable
   * c1  NUMBER             java.math.BigDecimal  39                    38            0         1       UInt64
   * c2  NUMBER             java.math.BigDecimal  39                    0             127       0       Decimal64(6)
   * c3  VARCHAR2           java.lang.String      2560                  2560          0         1
   * c4  DATE               java.sql.Timestamp    7                     0             0         1
   * c5  TIMESTAMP          oracle.sql.TIMESTAMP  11                    0             6         1
   */

  def apply(name: String,
            typeName: String,
            typeClass: String,
            displaySize: Int,
            precision: Int,
            scale: Int,
            isNullable: Int,
            notnull_columns: List[String]): OraChColumn = {

    val (nnLeftSide,nnRightSide) =
      if (isNullable == 1 && name.toLowerCase != "rn" && !notnull_columns.map(_.toLowerCase).contains(name.toLowerCase))
      ("Nullable(",")")
    else ("","")

    def getChCol: String =
      typeName match {
        case "NUMBER" => scale match {
          case 0 => "UInt64"
          case _ => "Decimal128(6)" //"Decimal64(6)"
        }
        case "VARCHAR2" => s"FixedString($precision)"
        case "DATE" => "DateTime"
        case "CLOB" => "String"
        case _ => "UNDEFINED_COL_TYPE"
      }

    val clColumnString: String = s"$name $nnLeftSide$getChCol$nnRightSide "

    new OraChColumn(
      name,
      typeName,
      typeClass,
      displaySize,
      precision,
      scale,
      isNullable,
      clColumnString
    )

  }
}