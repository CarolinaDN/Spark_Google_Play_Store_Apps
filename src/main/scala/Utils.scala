package org.jetbrains.scala

import java.io.{File, IOException}
import java.nio.file.{Files, StandardCopyOption}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.hadoop.io.IOUtils
import scala.reflect.io.Directory

object Utils {

  def load_df(path: String): DataFrame = {
    // Create SparkSession
    val spark = SparkSession.builder().master("local").appName("Google_Play_Store").getOrCreate()

    // Return dataframe from given path
    val df = (spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
      )

    // Print information about the dataframe
    print_dataFrame(df, path.split("/").last)

    return df

  }

  // Print information about a DataFrame
  def print_dataFrame(df : DataFrame, name : String) : Unit = {
    val banner = "*" * 20
    println("\n" + banner + " > DataFrame: " + name + " < " + banner + "\n")

    df.show(truncate = false)
    println("\nTotal number of rows: " + df.count())
    println("\nDataframe schema: ")
    df.printSchema()

    println(banner + " < END OF PRINT > " + banner)
  }

  // Save function for a DataFrame
  def save_DataFrame(df : DataFrame, base_path: String, filename: String, format : String, compression : String = "none", delimiter : String = ",") : Unit = {

    df.coalesce(1).write
      .format(format)
      .options(Map("header"->"true","delimiter"->delimiter, "compression"->compression))
      .mode("overwrite")
      .save(base_path + filename)

      // In order to write data to a single file

      // 1) rename new file
      val paths = get_Paths(base_path + filename, format)
      paths.head.renameTo(new File(filename))

      // 2) delete new directory
      val directory = new Directory(new File(base_path + filename))
      directory.deleteRecursively()

      // 3) move new file to the (originally) desired directory
      Files.move(new File(filename).toPath, new File(base_path + filename).toPath, StandardCopyOption.ATOMIC_MOVE)

      println("\n\t > " + filename + " < file was successfully created!")
  }

  // Returns a list of format file paths from within input directory
  private def get_Paths(read_directory: String, format: String) : List[File] = {
    val d = new File(read_directory)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(f => {f.isFile && f.toString.endsWith(format)}).toList}
    else {
      List[File]()
    }
  }

  // For each size String value, transform it into a Double value (in M)
  val convert_Size: UserDefinedFunction = udf { (size : String) =>
    if (size.endsWith("k"))
      size.dropRight(1).toDouble / 1000.0
    else if (size.endsWith("M"))
      size.dropRight(1).toDouble
    else
      0
  }

  // For each price String value, transform it into a Double value (in Euros)
  val convert_Price: UserDefinedFunction = udf { (price : String) =>
    if (price.startsWith("$")) {
      // conversion rate: 1$ = 0.9€
      (math rint price.substring(1).toDouble * 0.9 * 100) / 100
    }
    else
      0
  }

}