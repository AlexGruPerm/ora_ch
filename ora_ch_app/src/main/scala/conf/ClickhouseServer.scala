package conf

case class ClickhouseServer(
                            ip: String,
                            port: Int,
                            db: String,
                            batch_size: Int,
                            user: String,
                            password: String
                           ) {
  def getUrl: String =
    s"jdbc:clickhouse:http://$ip:$port/$db"
}
