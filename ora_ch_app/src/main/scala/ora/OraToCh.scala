package ora

import com.clickhouse.jdbc.ClickHouseConnection

import java.sql.{PreparedStatement, ResultSet}

object OraToCh {

  /**
   * create table data.eaist_lot
   * (
   * id                  UInt64 not null,
   * registrynumber      FixedString(19) not null,
   * tendersubject       FixedString(2048),
   * lotnumber           Float64 not null,
   * lotprice            Float64
   * )ENGINE = MergeTree()
   * PRIMARY KEY (id);
  */

  //https://clickhouse.com/docs/en/optimize/bulk-inserts
  def saveToClickhouse(table: String, oraRs: ResultSet, chConn: ClickHouseConnection, batchSize: Int): Unit = {
    val ps: PreparedStatement = chConn.prepareStatement(
      s"""insert into ${table}
         |select id, registrynumber, tendersubject, lotnumber, lotprice
         |from
         |input('id             UInt64,
         |       registrynumber FixedString(19),
         |       tendersubject  FixedString(2048),
         |       lotnumber      Decimal64(6),
         |       lotprice       Decimal64(6)
         |      ')
         |""".stripMargin)

      Iterator.continually(oraRs).takeWhile(_.next()).foldLeft(1){
        case (counter,rs) =>
        ps.setInt(1, rs.getInt("id"))
        ps.setString(2, rs.getString("registrynumber"))
        ps.setString(3, rs.getString("tendersubject"))
        ps.setDouble(4, rs.getDouble("lotnumber"))
        ps.setDouble(5, rs.getDouble("lotprice"))
        ps.addBatch()
          if (counter == batchSize) {
            ps.executeBatch()
            0
          } else
            counter+1
      }
    ps.executeBatch()
  }

}
