package conf

case class OraServer(
                      ip: String,
                      port: Int,
                      tnsname: String,
                      fetch_size: Int,
                      user: String,
                      password: String
                    ){
  def getUrl(): String =
    s"jdbc:oracle:thin:@//${ip}:${port}/${tnsname}"
}
