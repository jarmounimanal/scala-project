package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame
class Writer {

  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .option("header", "true")
      .mode(mode)
      .csv(path)
  }

  def writeParquet(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df.write
      .mode(mode)
      .parquet(path)
  }

  def writeTable(df: DataFrame, table: String, mode: String = "overwrite", tablePath: String): Unit = {
    df.write
      .mode(mode)
      .option("path", tablePath)
      .saveAsTable(table)
  }

}
