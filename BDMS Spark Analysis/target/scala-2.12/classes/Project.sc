import org.apache.spark.sql.{SparkSession, DataFrame}

val spark = SparkSession.builder()
  .appName("MySQL Spark Example")
  .config("spark.master", "local") // Change this according to your Spark cluster configuration
  .getOrCreate()

val url = "jdbc:mysql://localhost:3306/BDMS" // Replace with your MySQL database URL
val user = "root" // Replace with your MySQL username
val password = "root" // Replace with your MySQL password
val tableName = "stock_data" // Replace with the name of your MySQL table

val jdbcDF: DataFrame = spark.read
  .format("jdbc")
  .option("url", url)
  .option("dbtable", tableName)
  .option("user", user)
  .option("password", password)
  .load()

jdbcDF.show()
