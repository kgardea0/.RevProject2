package retail_db
import vegas._
import vegas.data.External._
import org.apache.spark.sql.SparkSession

object questin_1a {

  val mycsv = "C:\\Users\\Justin\\Desktop\\spark3demo\\src\\input\\complete_csv.csv "

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Question 1A")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(mycsv)

    df.createOrReplaceTempView("complete_csv")


    val querydf =spark.sql("SELECT product_category, SUM(price) FROM complete_csv GROUP BY product_category ORDER BY product_category DESC")
    querydf.show()

    df.show()
  }
}
