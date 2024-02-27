package org.jetbrains.scala

import java.nio.file.Paths
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import org.jetbrains.scala.Utils.print_dataFrame

/* Some references:
https://medium.com/codex/scala-functional-programming-with-spark-datasets-e470f48fcc11
https://sparkbyexamples.com/spark/spark-read-csv-file-into-dataframe/
https://spark.apache.org/docs/latest/sql-getting-started.html

 */

object Google_Files_Challenge {

  def main(args: Array[String]): Unit = {

    // Beforehand csv files have been unzipped and stored in local project directory
    val base_path = Paths.get("").toAbsolutePath + "/google-play-store-apps/"
    val googleplaystore = Utils.load_df(base_path + "googleplaystore.csv")
    val user_reviews = Utils.load_df(base_path + "googleplaystore_user_reviews.csv")

    // Call functions for each part of the exercise
    val df_1 = part1(user_reviews)
    print_dataFrame(df_1, "df_1")
    part2(googleplaystore, base_path)
    val df_3 = part3(googleplaystore)
    part4(df_1, df_3, base_path)
    part5(user_reviews, df_3, base_path)
  }

  private def part1(user_reviews: DataFrame): DataFrame = {

    val df_1 = user_reviews
      // "Sentiment_Polarity" column: cast String to Double and replace NULL by 0
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double"))
      .withColumn("Sentiment_Polarity", when(col("Sentiment_Polarity").isNull || col("Sentiment_Polarity").isNaN, 0).otherwise(col("Sentiment_Polarity")))

      // & Average of the column Sentiment_Polarity grouped by App name
      .groupBy("App")
      .agg(mean("Sentiment_Polarity"))
      .withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")

    return df_1
  }

  private def part2(googleplaystore: DataFrame, base_path: String): Unit = {
    // Apps with a "Rating" greater or equal to 4.0 sorted in descending order.
    // OBS: there are ratings higher than 5
    val df_2 = googleplaystore
      .withColumn("Rating", col("Rating").cast("double"))
      .filter(googleplaystore("Rating") >= 4)
      .orderBy(desc("Rating"))

    print_dataFrame(df_2, "best_apps")
    Utils.save_DataFrame(df_2, base_path, "best_apps.csv", "csv", delimiter="§")
  }


  private def part3(googleplaystore: DataFrame): DataFrame = {
    val df_3_clean = googleplaystore
      // Rating: change data type to double
      .withColumn("Rating", col("Rating").cast("double"))

      // Reviews: change datatype to long and correct default values
      .withColumn("Reviews", col("Reviews").cast("long"))
      .withColumn("Reviews", when(col("Reviews").isNaN || col("Reviews").isNull, 0).otherwise(col("Reviews")))

      // Size: convert_Size, cast String to Double and correct default values
      .withColumn("Size", Utils.convert_Size(col("Size")))
      .withColumn("Size", col("Size").cast("double"))
      .withColumn("Size", when(col("Size") === 0.0, lit(null)).otherwise(col("Size")))

      // Price: cast String to Double, convert values to Euros and correct default values
      .withColumn("Price", Utils.convert_Price(col("Price")))
      .withColumn("Price", col("Price").cast("double"))
      .withColumn("Price", when(col("Price") === 0.0, lit(null)).otherwise(col("Price")))

      // Rename columns
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")

    val df_3_group = df_3_clean
      // Obtain the row with max reviews per App and all possible categories
      .groupBy("App")
      .agg(collect_set("Category"), max("Reviews"))
      .withColumnRenamed("collect_set(Category)", "Categories")
      .withColumnRenamed("max(Reviews)", "Reviews")

    // Retrieve the remaining data columns and obtain df_3
    var df_3 = df_3_clean
      .join(df_3_group, Seq("App", "Reviews"))
      .dropDuplicates("App")
      .drop("Category")

    // Replace all NaN values with null values (except for "Categories" column)
    for (column <- df_3.columns)
      if(!column.equals("Categories"))
        df_3 = df_3.withColumn(column, when(col(column).isNaN, lit(null)).otherwise(col(column)))

    df_3 = df_3
      // "Genres" column: split string into array of strings
      .withColumn("Genres", split(col("Genres"), ";"))

      // Convert string to date
      .withColumn("Last_Updated", to_timestamp(col("Last_Updated"), "MMMM d, y"))
      .withColumn("Last_Updated", col("Last_Updated").cast(sql.types.DataTypes.DateType))

    Utils.print_dataFrame(df_3, "df_3")
    return df_3
  }

  private def part4(df_1 : DataFrame, df_3 : DataFrame, base_path: String) : Unit = {
    val df_ = df_3.join(df_1, Seq("App"), "outer")

    Utils.print_dataFrame(df_, "googleplaystore_cleaned")
    Utils.save_DataFrame(df_, base_path, "googleplaystore_cleaned.parquet", "parquet", compression="gzip")
  }

  private def part5(user_reviews: DataFrame, df_3: DataFrame, base_path: String) : Unit = {
    // Genres: unfold each genre item in the array into a new row and group by
    var df_4 = df_3
      .join(
        user_reviews
          .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double"))
          .withColumn("Sentiment_Polarity",
            when(col("Sentiment_Polarity").isNull || col("Sentiment_Polarity").isNaN, 0).otherwise(col("Sentiment_Polarity"))),
        Seq("App"), "left")

      .withColumn("Genres", explode(col("Genres")))
      .groupBy("Genres")
      .agg(count("Genres"), mean("Rating"), mean("Sentiment_Polarity"))

      // Rename columns
      .withColumnRenamed("Genres", "Genre")
      .withColumnRenamed("count(Genres)", "Count")
      .withColumnRenamed("avg(Rating)", "Average_Rating")
      .withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")

    Utils.print_dataFrame(df_4, "df_4")
    Utils.save_DataFrame(df_4, base_path, "googleplaystore_metrics.parquet", "parquet", compression = "gzip")

  }
}
