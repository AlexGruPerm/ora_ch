package common

import java.sql.ResultSet

case class ResultSetWithQuery(rs: ResultSet, query: String)
