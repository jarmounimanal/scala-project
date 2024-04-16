package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .load(path)
  }
  def readParquet(path: String): DataFrame = {
    sparkSession
      .read
      .format("parquet")
      .load(path)
  }

  def readTable(table: String, location: String): DataFrame = {
    sparkSession
      .read
      .format("parquet")
      .option("basePath", location)
      .load(location + "/" + table)
  }


  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation")
  }

}
