package retail_db

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, DslSymbol, StringToAttributeConversionHelper}
import org.apache.spark.sql.types._

object data_cleanup {

  val mycsv = "src/input/CSV_Data_2022_5_20_19_58.csv"


  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("data_cleanup")
      .config("spark.master", "local")
      .getOrCreate()



    var df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(mycsv)

    df.show(20)

//change order_id column to sequential integers by iterating over a range of integers
//    val x = Range(1,10001)
//    x.foreach((i: Int) =>
//    df = df.withColumn("order_id",i)
//    )

//change columns to integer values
    df = df.withColumn("qty",col("qty").cast("Integer"))
    df.printSchema()



    //    df
//      .filter(df("country") === " British" || df("country") === " U.S.")
//      .show()

//    df("country")


    //  df.createOrReplaceTempView("ecommerce")


//  val sqlDF = spark.sql("SELECT * FROM ecommerce")
//  sqlDF.show

  }
}
