package fr.mosef.scala.template
import org.apache.spark.sql.SaveMode

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem

object Main extends App with Job {

  val cliArgs = args
  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]"
  }

  //Upload_data
  val SRC_PATH: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./src/main/resources/drivers.csv"
    }
  }

  val SRC_PATH_PARQUET: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./src/main/resources/data.parquet"
    }
  }

  // Path des destinations
  val DST_PATH: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer"
    }
  }

  val DST_PATH_PARQUET: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer-parquet"
    }
  }

  val conf = new SparkConf()
  conf.set("spark.driver.memory", "64M")
  conf.set("spark.testing.memory", "471859200")

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .config(conf)
    .appName("Scala Template")
    .enableHiveSupport()
    .getOrCreate()
  
  sparkSession
    .sparkContext
    .hadoopConfiguration
    .setClass("fs.file.impl",  classOf[BareLocalFileSystem], classOf[FileSystem])

//Initialisation

  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new Writer()

// Attribue le chemin du fichier source pour les données CSV
  val src_path = SRC_PATH
  // Attribue le chemin du fichier source pour les données parquet
  val src_path_parquet = SRC_PATH_PARQUET

  // Attribue le chemin du fichier destination pour les données CSV
  val dst_path = DST_PATH
  // Attribue le chemin du fichier destination pour les données parquet
  val dst_path_parquet = DST_PATH_PARQUET

//fonction reader
  val inputDF = reader.read(src_path)
  inputDF.show(60)
  val inputDFparquet = reader.readParquet(src_path_parquet)
  inputDFparquet.show(60)

  //fonction processor
  val processedDF: DataFrame = processor.process(inputDF)
  val processedDF_parquet = processor.countRows(inputDFparquet)

//write
  writer.write(processedDF, "overwrite", dst_path)
  writer.writeParquet(processedDF_parquet, "overwrite", dst_path_parquet)



//Hive

  val table = "table"
  val tableLocation = "./src/main/ressources"
  processedDF.write
    .mode(SaveMode.Overwrite)
    .option("path", tableLocation)
    .saveAsTable(table)



  val hiveTableName = "table"
  val hiveTableLocation = "./src/main/ressources"
  val hiveTableDF = reader.readTable(hiveTableName, hiveTableLocation)
  hiveTableDF.show(60)

  val columnName = "sum(value)"
  val processedDF_hive = processor.sum(hiveTableDF, columnName)

//ecriture de la table
  val tableNametoload = "table_loaded"
  val tablePath = "./default/output-writer-hive"
  writer.writeTable(processedDF_hive, tableNametoload, tablePath = tablePath)




}
