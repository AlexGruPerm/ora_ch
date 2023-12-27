package conf

case class ClickhouseServer(
                            ip: String,
                            port: Int,
                            batch_size: Int,
                            user: String,
                            password: String
                           )
