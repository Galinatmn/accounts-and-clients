package org.example
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._

import scala.language.postfixOps


object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
    //  .appName("ClientsAndAccounts")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")



    val schema = new StructType()
      .add(name = "ClientId", StringType)
      .add(name = "ClientName", StringType)
      .add(name = "Type", StringType)
      .add(name = "Form", StringType)
      .add(name = "RegisterDate", DateType)

    val clientPath = "C:/Users/petrova/IdeaProjects/project1/data/Clients1.csv"

    val clientDf = spark.read.option("delimiter", ";")
      .option("header", "true")
      .schema(schema)
      .csv(clientPath)

   // clientDf.show(5)

    //Создаем датафрейм с данными из таблицы Account.csv
    val accountPath = "C:/Users/petrova/IdeaProjects/project1/data/Account1.csv"

    val accountSchema = new StructType()
      .add(name = "AccountID", StringType)
      .add(name = "AccountNum", StringType)
      .add(name = "ClientId", StringType)
      .add(name = "DateOpen", DateType)

    var accountDf = spark.read.option("delimiter", ";")
      .option("header", "true")
      .schema(accountSchema)
      .csv(accountPath)
    accountDf = accountDf.withColumn("Col", lit("123"))
   // accountDf.show(10)

    //Создаем датафрейм с данными из таблицы Operation.csv
    val operationPath = "C:/Users/petrova/IdeaProjects/project1/data/Operation1.csv"

    var operationSchema = new StructType()
      .add(name = "AccountDB", StringType)
      .add(name = "AccountCR", StringType)
      .add(name = "DateOp", DateType)
      .add(name = "Amount", DoubleType)
      .add(name = "Currency", StringType)
      .add(name = "Comment", StringType)

    var operationDf = spark.read.format("csv")
      //.option("path", "file: //C:/Users/petrova/IdeaProjects/project1/data/Operation.csv")
      .option("delimiter", ";")
      .option("header", "true")
      .schema(operationSchema)
      .csv(operationPath)
     // .show()

      //operationDf.show(5)option("header", "true")


    //Создаем датафрейм с данными из таблицы Rate.csv
    val ratePath = "C:/Users/petrova/IdeaProjects/project1/data/Rate.csv"

    val rateSchema = new StructType()
      .add(name = "Currency", StringType)
      .add(name = "Rate", DoubleType)
      .add(name = "RateDate", DateType)

    val rateDf = spark.read.option("delimiter", ";")
      .option("header", "true")
      .schema(rateSchema)
      .csv(ratePath)



    //2020-11-01

    val dfAccountOperation = accountDf.join(operationDf, accountDf("AccountID") === operationDf("AccountDB"), joinType = "Inner")
      .select("AccountDB", "AccountID", "Amount")
      .withColumn("first", when(col("AccountDB") === 13 || col("AccountDB") === 17, value = 1)
                                    .when(col("AccountDB") === 1 && col("AccountID") === 1, value = 0)
                                    .otherwise("Unknouwn")
      )
      .where(operationDf("DateOp") === "2020-11-01")

    //Переводим денежные единицы в рубли

    val operationActualDf = operationDf.withColumn("AmountActual", when(col("Currency") === "RUB", value = operationDf("Amount"))
      .when(col("Currency") === "USD", value = operationDf("Amount") * 70)
      .when(col("Currency") === "EU", value = operationDf("Amount") * 75)
    )

    //СОздаем подКЛЮЧЕНИЕ К БД

     val jdbcDf = spark.read
       .format("jdbc")
       .option("url", "jdbc:postgresql://localhost:5432/postgres")
       .option("dbtable", "transaction.list")
       .option("user", "postgres")
       .option("password", "postgres")
       .load()

       //Присваиваем объектам значения массива из БД

    val listValues = Array(jdbcDf("listone"))

    val listValuesTwo = Array(jdbcDf("listtwo"))

    //Создаем промежуточные объекты для построения витрин

    val PaymentAmt = operationActualDf.filter(operationDf("DateOp") === "2020-11-01")
      .groupBy("AccountDB")
      .agg(
        functions.sum("AmountActual").as("PaymentAmt")
      )

    //PaymentAmt.printSchema()

    val EnrollementAmt = operationActualDf.filter(operationDf("DateOp") === "2020-11-01")
      .groupBy("AccountCR")
      .agg(
        functions.sum("AmountActual").as("EnrollementAmt")
      )
    //EnrollementAmt.printSchema()

    val TaxAmt = operationActualDf.filter(operationDf("AccountCR") === 40702 && operationDf("DateOp") === "2020-11-01")
      .groupBy("AccountDB")
      .agg(
        functions.sum("AmountActual").as("TaxAmt")
      )
     // TaxAmt.printSchema()

    val ClearAmt = operationActualDf.filter(operationActualDf("AccountDB") === 40802 && operationActualDf("DateOp") === "2020-11-01")
      .groupBy("AccountCR")
      .agg(
        functions.sum("Amount").as("ClearAmt")
      )
     // ClearAmt.printSchema()

    val CarsAmt = operationActualDf.filter(!operationActualDf("Comment").isin("listValues:_*") && operationActualDf("DateOp") === "2020-11-01")
      .groupBy("AccountDB")
      .agg(
        functions.sum("AmountActual").as("CarsAmt")
      )
     // CarsAmt.printSchema()

    val FoodAmt = operationActualDf.filter(operationActualDf("Comment").isin("listValuesTwo:_*") && operationActualDf("DateOp") === "2020-11-01")
      .groupBy("AccountCR")
      .agg(
        functions.sum("AmountActual").as("FoodAmt")
      )
     // .show()
      //FoodAmt.printSchema()

    val FLAmt =   accountDf.join(operationActualDf, accountDf("AccountID") === operationActualDf("AccountDB"), joinType = "Inner")
      .join(clientDf, accountDf("ClientId") === clientDf("ClientId"), joinType = "Inner")
//      .withColumn("DateOp", col("DateOp").as("CutoffDt"))
      .filter(clientDf("Type") === "Ф" && operationActualDf("DateOp") === "2020-11-01")
      .groupBy("AccountDB", "DateOp")
      .agg(
        functions.sum("AmountActual").as("FLAmt"))
      //.show()
      //FLAmt.printSchema()


    //первая витрина corporatePayments

    val corporatePayments = accountDf
      .join(PaymentAmt, accountDf("AccountID") === PaymentAmt("AccountDB"), joinType = "Left")
      .join(EnrollementAmt, accountDf("AccountID") === EnrollementAmt("AccountCR"), joinType = "Left")
      .join(TaxAmt, accountDf("AccountID") === TaxAmt("AccountDB"), joinType = "Left")
      .join(ClearAmt, accountDf("AccountID") === ClearAmt("AccountCR"), joinType = "Left")
      .join(CarsAmt, accountDf("AccountID") === CarsAmt("AccountDB"), joinType = "Left")
      .join(FoodAmt, accountDf("AccountID") === FoodAmt("AccountCR"), joinType = "Left")
      .join(FLAmt, accountDf("AccountID") === FLAmt("AccountDB"), joinType = "Left")
      .select("AccountID", "ClientId", "PaymentAmt", "EnrollementAmt", "TaxAmt", "ClearAmt", "CarsAmt", "FoodAmt", "FLAmt", "DateOp")
      //.show(30)

    //промежуточный объект

    val TotalAmt = EnrollementAmt
      .join(PaymentAmt, EnrollementAmt("AccountCR") === PaymentAmt("AccountDB"), joinType = "Left")
      .withColumn("TotalAmt", PaymentAmt("PaymentAmt"))

      // TotalAmt.printSchema()

      //Витрина corporateAccount

    val corporateAccount = accountDf
      .join(TotalAmt, accountDf("AccountID") === TotalAmt("AccountDB"), joinType = "Left")
      .join(clientDf, accountDf("ClientId") === clientDf("ClientId"), joinType = "Left")
      .select(accountDf("AccountID"), accountDf("AccountNum"), accountDf("DateOpen"), clientDf("ClientId"), clientDf("ClientName"), TotalAmt("TotalAmt"))
    // .show(30)

    //Витрина corporateInfo

    val corporateInfo = clientDf
      .join(corporateAccount, clientDf("ClientId") === corporateAccount("ClientId"), joinType = "Left")
      .select(clientDf.as("cDf")("ClientId"), clientDf.as("cDf")("ClientName"),clientDf("Type"), clientDf("Form"), clientDf("RegisterDate"), corporateAccount("TotalAmt"))
      //.show(3)

    // Сохраняем витрины в паркет файлы

    corporatePayments.write
     .parquet("C:/Users/petrova/Desktop/result/corporatePayments.parquet")

    corporateAccount.write
          .parquet("C:/Users/petrova/Desktop/result/corporateAccount.parquet")

    corporateInfo.write
         .parquet("C:/Users/petrova/Desktop/result/corporateInfo.parquet")

  }

}
