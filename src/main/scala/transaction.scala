import org.apache.spark.rdd
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.

object RDDParallelize {
  def main(args: Array[String]): Unit = {
  val spark:SparkSession =  SparkSession.builder().master(master = "local")
    .appName(name = "Transaction")
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.5.1.jar")
    .getOrCreate()

    //Создаем датафрейм с данными из таблицы Clients.csv
    val clientPath = "/home/jupyter-petrova_galina/Clients.csv"

    val schema = new StructType()
      .add(name = "ClientId", IntegerType)
      .add(name = "ClientName", StringType)
      .add(name = "Type", StringType)
      .add(name = "Form", StringType)
      .add(name = "RegisterDate", DateType)

    val clientDf = spark.read.option("delimiter", ";")
      .option("header", "true")
      .schema(schema)
      .csv(clientPath)

    //Создаем датафрейм с данными из таблицы Account.csv
    val accountPath = "/home/jupyter-petrova_galina/Account.csv"

    val accountSchema = new StructType()
      .add(name = "AccountID", IntegerType)
      .add(name = "AccountNum", StringType)
      .add(name = "ClientId", IntegerType)
      .add(name = "DateOpen", DateType)

    val accountDf = spark.read.option("delimiter", ";")
      .option("header", "true")
      .schema(accountSchema)
      .csv(accountPath)

    //Создаем датафрейм с данными из таблицы Operation.csv
    val operationPath = "/home/jupyter-petrova_galina/Operation.csv"

    var operationSchema = new StructType()
      .add(name = "AccountDB", IntegerType)
      .add(name = "AccountCR", IntegerType)
      .add(name = "DateOp", DateType)
      .add(name = "Amount", FloatType)
      .add(name = "Currency", StringType)
      .add(name = "Comment", StringType)

    var operationDf = spark.read.option("delimiter", ";")
      .option("header", "true")
      .schema(operationSchema)
      .csv(operationPath)

    //Создаем датафрейм с данными из таблицы Rate.csv
    val ratePath = "/home/jupyter-petrova_galina/Rate.csv"

    val rateSchema = new StructType()
      .add(name = "Currency", StringType)
      .add(name = "Rate", FloatType)
      .add(name = "RateDate", DateType)

    val rateDf = spark.read.option("delimiter", ";")
      .option("header", "true")
      .schema(rateSchema)
      .csv(ratePath)

    operationDf.where(operationDf("DateOp") === "2020-11-01").show(truncate = false)

    val showcaseOne = operationDf.join(rateDf, operationDf("Currency") === rateDf("Currency"), joinType = "inner")
      .withColumn(colName = "PaymentAmt", when(col(colName = "Currency") === "USD", value = 80))
      .show(truncate = false)
      .where(operationDf("DateOp") === "2020-11-01").show(truncate = false)

  }
}
