package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.execution.datasources.truncate

object transaction {
  def main(args: Array[String]): Unit = {
  val spark:SparkSession =  SparkSession.builder().master("local[*]")
    .appName("Transaction")
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.5.1.jar")
    .getOrCreate()

    //Создаем датафрейм с данными из таблицы Clients.csv
    val clientPath = "C:/Users/petrova/IdeaProjects/project1/data/Clients1.csv"

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

    clientDf.show(5)

    val ratePath = "C:/Users/petrova/IdeaProjects/project1/data/Rate.csv"

    val rateSchema = new StructType()
      .add(name = "Currency", StringType)
      .add(name = "Rate", DoubleType)
      .add(name = "RateDate", DateType)

    val rateDf = spark.read.option("delimiter", ";")
      .option("header", "true")
      .schema(rateSchema)
      .csv(ratePath)
      //.show()

    val operationPath = "C:/Users/petrova/IdeaProjects/project1/data/Operation1.csv"

    var operationSchema = new StructType()
      .add(name = "AccountDB", StringType)
      .add(name = "AccountCR", StringType)
      .add(name = "DateOp", DateType)
      .add(name = "Amount", DoubleType)
      .add(name = "Currency", StringType)
      .add(name = "Comment", StringType)

    var operationDf = spark.read.format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .schema(operationSchema)
      .csv(operationPath)
      //.show()

    operationDf.withColumn("AmountActual", when(col("Currency") === "RUB", value = operationDf("Amount"))
      .when(col("Currency") === "USD", value = operationDf("Amount") * 70)
      .when(col("Currency") === "EU", value =  operationDf("Amount") * 75)
    ).show()

//    ("first", when(col("AccountDB") === 13 || col("AccountDB") === 17, value = 1)
//      .when(col("AccountDB") === 1 && col("AccountID") === 1, value = 0z)
//      .otherwise("Unknouwn")

    val rateend = rateDf.filter(rateDf("Currency") === "USD" && rateDf("RateDate") === "2020-01-02").show()



    //Создаем датафрейм с данными из таблицы Account.csv
//    val accountPath = "C:/Users/petrova/IdeaProjects/project1/data/Account.csv"
//
//    val accountSchema = new StructType()
//      .add(name = "AccountID", IntegerType)
//      .add(name = "AccountNum", StringType)
//      .add(name = "ClientId", IntegerType)
//      .add(name = "DateOpen", DateType)
//
//    val accountDf = spark.read.option("delimiter", ";")
//      .option("header", "true")
//      .schema(accountSchema)
//      .csv(accountPath)
//
//    //Создаем датафрейм с данными из таблицы Operation.csv
//    val operationPath = "C:/Users/petrova/IdeaProjects/project1/data/Operation.csv"
//
//    var operationSchema = new StructType()
//      .add(name = "AccountDB", IntegerType)
//      .add(name = "AccountCR", IntegerType)
//      .add(name = "DateOp", DateType)
//      .add(name = "Amount", FloatType)
//      .add(name = "Currency", StringType)
//      .add(name = "Comment", StringType)
//
//    var operationDf = spark.read.option("delimiter", ";")
//      .option("header", "true")
//      .schema(operationSchema)
//      .csv(operationPath)
//
//    //Создаем датафрейм с данными из таблицы Rate.csv
//    val ratePath = "C:/Users/petrova/IdeaProjects/project1/data/Rate.csv"
//
//    val rateSchema = new StructType()
//      .add(name = "Currency", StringType)
//      .add(name = "Rate", FloatType)
//      .add(name = "RateDate", DateType)
//
//    val rateDf = spark.read.option("delimiter", ";")
//      .option("header", "true")
//      .schema(rateSchema)
//      .csv(ratePath)
//
//    operationDf.where(operationDf("DateOp") === "2020-11-01").show(truncate = false)
//
//    showcaseOne = operationDf.join(rateDf, operationDf("Currency") === rateDf("Currency"), joinType = "inner")
//      .withColumn(colName = "PaymentAmt", when(col(colName = "Currency") === "USD", value = 80))
//      .where(operationDf("DateOp") === "2020-11-01").show(truncate = false)
//      .show(truncate = false)

    //    val distinctDf = rateDf.distinct()
    //
    //    val distinctDf2 = rateDf.dropDuplicates("Currency").show(5)
    //
    //    val rateMaxDate = rateDf.groupBy("Currency").count()
    //      .filter(rateDf("Currency") === "USD" || rateDf("Currency") === "RU" && rateDf("Currency") === "1")

    //    val filterComment = operationDf.filter(array_contains(operationDf("Comment") === "er"))


    //    val corporatePayments = accountDf.join(operationDf, accountDf("AccountID") === operationDf("AccountDB"), joinType = "Inner")
    //      .select( "AccountID", "ClientId", "Amount", "AccountDB", "AccountCR")
    //      .withColumn("Amount", col("Amount").cast(to = "float"))
    //      .groupBy("ClientId", "AccountID", "AccountCR", "AccountDB")
    //      .agg(
    //        functions.sum("Amount").as("PaymentAmt"),
    //        functions.sum("Amount").as("EnrollementAm")
    //      )
    //      .show(5)
    //println(jdbcDf.show(2))

    //    val ddf1 = jdbcDf.filter(array_contains(jdbcDf("listone"), "%соя%"))
    //      .show()

    // operationDf.show(5)

  }
}
