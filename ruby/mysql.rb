require "mysql2"

MySQL = Mysql2::Client.new(
  host: ENV.fetch("MYSQL_HOST", "localhost"),
  username: "root",
)
