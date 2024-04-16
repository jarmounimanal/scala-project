package fr.mosef.scala.template.processor.impl
import org.apache.spark.sql.{DataFrame, functions}

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("nationality").sum("driverId")
  }
  def countRows(dataFrame: DataFrame): DataFrame = {
    val rowCount = dataFrame.count()
    val spark = dataFrame.sparkSession
    import spark.implicits._
    val countDF = Seq(rowCount).toDF("rowCount")
    countDF
  }

  def sum(dataFrame: DataFrame, columnName: String): DataFrame = {
    val sumResult = dataFrame.agg(functions.sum(columnName)).toDF("sumResult")
    sumResult
  }
}
