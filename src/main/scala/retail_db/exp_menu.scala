package retail_db

/*IMPORT PACKAGES*/

import org.apache.spark.sql.{DataFrame, SparkSession}
import vegas.DSL.Vegas
import vegas.sparkExt.VegasSpark
import vegas.{Area, Bar, Nom, Nominal, Quant, Scale}
import scala.Console.println
import scala.io.StdIn.readLine
import scala.language.postfixOps
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*TEAM VEGAS_P2 OBJECT CREATED*/
object exp_menu {
  def main(args: Array[String]): Unit = {

    /*TURN LOG OFF*/
    Logger.getLogger("org").setLevel(Level.ERROR)

    /*ESTABLISHING VEGAS SPARK SESSION CONNECTION
        FOR QUERIES VISUALIZATION*/
    val spark: SparkSession = SparkSession
      .builder()
      .appName("TeamVegas Data Visualizer")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    /*ESTABLISHING SPARK SESSION CONNECTION
      FOR SPARK SQL QUERIES*/
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("TeamVegas")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*CONVERTING DATASET TO DATAFRAME USING
    SPARK SESSION CONNECTION*/
    val orders: DataFrame = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/Input/complete_csv.csv")
      .toDF("order_id", "customer_id", "customer_name", "product_id", "product_name",
        "product_category", "payment_type", "qty", "product_price", "datetime", "country",
        "city", "website", "txn_id", "txn_success", "failure_reason")
    orders.createOrReplaceTempView("orders")

    /*DEFINING VARIABLES FOR INT AND STRING*/
    var TeamMemberLogin = "JustY"
    var AdministrativeLogin = "1234"
    var SprintLog = "Phase"
    var LogOut = "Y"
    var input = "0"
    println("--" * 50)


    /*BLOCK 1 ADMINISTRATIVE LOG IN OPTION*/
    /*MENU OPTIONS*/
    do {
      print(
        """VEGAS ANALYTICAL APPLICATION
          |
          |[1] Administrative Login
          |[2] Analysis Login
          |[3] Log out
          |
          |CHOOSE AN OPTION:""".stripMargin)
      input = readLine
      input match {
        case "1" =>
          do {
            println("Admin login: ")
            AdministrativeLogin = readLine
            AdministrativeLogin match {
              case "1234" =>

                /*IF CONDITIONAL STATEMENT NESTED IN DO WHILE LOOP
                TO CREATE FUNCTIONAL ADMINISTRATIVE LOG IN OPTIONS*/
                if (input == "1" && AdministrativeLogin == "1234") {
                  println("Successful Login")
                  println("--" * 50)

                  /*SPRINT LOG TEAM MEMBERS AND TASK*/
                  println(
                    """PLANNING/ ANALYSIS/ IMPLEMENTATION PHASE
                      |SPRINT TEAM MEMBER
                      |Justin Yeary
                      |Kiara Gardea
                      |
                      |TESTING/ DEPLOYMENT/ RETROSPECT PHASE
                      |SPRINT TEAM MEMBER
                      |Trathen Mecimore
                      |Nicholas Famoye
                      |
                      |PLEASE LOGIN TO VIEW ANALYSIS
                      |""".stripMargin)
                }
              case _ =>
                println("Wrong Entry, Enter Your Password !!")
            }
          } while (AdministrativeLogin != "1234")
          println("--" * 50)

        /*BLOCK 2 Team LOG IN OPTION*/
        case "2" =>
          do {
            println("Team Login: ")
            TeamMemberLogin = readLine
            TeamMemberLogin match {
              case "JustY" =>

                /*IF CONDITIONAL STATEMENT NESTED IN DO WHILE LOOP
                TO CREATE FUNCTIONAL TEAM LOG IN OPTIONS*/
                if (input == "2" && TeamMemberLogin == "JustY") {
                  println("Successful Login")
                  println("--" * 50)

                  /*SPRINT LOG TASK*/
                  println(
                    """
                      |PRODUCT OWNERS BACKLOG ACCOMPLISHED 📌
                      |A. What is the top selling category of items Per Country
                      |B. How does the popularity of products change throughout the year Per Country
                      |C. Which locations see the highest traffic of sales
                      |D. What times have the highest traffic of sales? Per Country
                      |
                      |Big Data Analysis!
                      |""".stripMargin)
                  println("--" * 50)
                }
              case _ =>
                println("Wrong Entry, Enter Your Password !!")
            }
          } while (TeamMemberLogin != "JustY")

          /*NESTED DO WHILE LOOP WITHIN A DO WHILE LOOP
       TO FILTER SPRINT LOG OPTIONS*/
          do {
            print("What Sprint BackLog Would You Like To Analyze? ")
            SprintLog = readLine
            println("--" * 50)
            SprintLog match {

              /*QUERY 1 SPARK SQL BLOCK*/
              case "A" =>
                println("BIG DATA ANALYSIS")
                println(
                  """
                    |Query Output
                    |""".stripMargin)

                /*IF CONDITIONAL STATEMENT NESTED IN DO WHILE LOOP
                TO CREATE FUNCTIONAL LOG IN OPTIONS*/
                if (input == "2" && SprintLog == "A") {

                  /*QUERY 1A SPARK SQL*/
                  val dfQ1a = sparkSession.sql(
                    "SELECT product_category, SUM(qty) " +
                      "FROM orders GROUP BY product_category")
                    .toDF("Category", "Products Sold")
                    .sort("Category")

                  /*QUERY 1A VEGAS VISUALIZATION*/
                  Vegas("categories of products sold")
                    .withDataFrame(dfQ1a)
                    .mark(Bar)
                    .encodeX("Category", Nom)
                    .encodeY("Products Sold", Quant)
                    .encodeColor("Category", Nominal, scale = Scale(rangeNominals =
                      List("#f21beb", "#1b49f2", "#1bf25c", "f3fa00", " #fa0b07", "#07faf1")))
                    .show
                  dfQ1a.show()

                  /*QUERY 1B SPARK SQL*/
                  val dfQ1b = sparkSession.sql(
                    "SELECT country, product_category, SUM(qty) " +
                      "FROM orders GROUP BY country, product_category LIMIT 6")
                    .toDF("Country","Category","Products Sold")
                    .sort("Country", "Category")

                  /*QUERY 1B VEGAS VISUALIZATION FOR*/
                  Vegas("categories of products sold")
                    .withDataFrame(dfQ1b)
                    .mark(Bar)
                    .encodeX("Country", Nom)
                    .encodeY("Products Sold", Quant)
                    .show
                  dfQ1b.show()
                  println(
                    """
                      |1st Task Accomplished
                      |""".stripMargin)
                }
                println("--" * 50)

              /*QUERY 2 SPARK SQL BLOCK*/
              case "B" =>
                println("BIG DATA ANALYSIS")
                println(
                  """
                    |Query Output
                    |""".stripMargin)
                if (input == "2" && SprintLog == "B") {

                  /*QUERY 2A SPARK SQL*/
                  val dfQ2a = sparkSession.sql(
                    "SELECT QUARTER(datetime), SUM(qty) " +
                      "FROM orders GROUP BY QUARTER(datetime)")
                    .toDF("Quarter", "Total Sold")
                    .sort("Total Sold")

                  /*QUERY 2A VEGAS VISUALIZATION*/
                  Vegas("Seasonal popularity of products")
                    .withDataFrame(dfQ2a)
                    .mark(Area)
                    .encodeX("Quarter", Nom)
                    .encodeY("Total Sold", Quant)
                    .show
                  dfQ2a.show()

                  /*QUERY 2B SPARK SQL*/
                  val dfQ2b = sparkSession.sql(
                    "SELECT country, QUARTER(datetime), SUM(qty) " +
                      "FROM orders GROUP BY country, QUARTER(datetime) LIMIT 5")
                    .toDF("Country", "Quarter", "Total Sold")
                    .sort("Country", "Total Sold")

                  /*QUERY 2B VEGAS VISUALIZATION*/
                  Vegas("Seasonal popularity of products by country")
                    .withDataFrame(dfQ2b)
                    .mark(Area)
                    .encodeX("Country", Nom)
                    .encodeY("Total Sold", Quant)
                    .show
                  dfQ2b.show()
                  println(
                    """
                      |2nd Task Accomplished
                      |""".stripMargin)
                }
                println("--" * 50)

              /*QUERY 3 SPARK SQL BLOCK*/
              case "C" =>
                println("BIG DATA ANALYSIS")
                println(
                  """
                    |Query Output
                    |""".stripMargin)
                if (input == "2" && SprintLog == "C") {

                  /*QUERY 3 SPARK SQL*/
                  val dfQ3 = sparkSession.sql(
                    "SELECT city, SUM(qty) " +
                      "FROM orders GROUP BY city LIMIT 6")
                    .toDF("City","Number of Visitors")
                    .sort("Number of Visitors")

                  /*QUERY 3 VEGAS VISUALIZATION*/
                  Vegas("Busiest locations")
                    .withDataFrame(dfQ3)
                    .mark(Bar)
                    .encodeX("City", Nom)
                    .encodeY("Number of Visitors", Quant)
                    .show
                  dfQ3.show()
                  println(
                    """
                      |3rd Task Accomplished
                      |""".stripMargin)
                }
                println("--" * 50)

              /*QUERY 4 SPARK SQL BLOCK*/
              case "D" =>
                println("BIG DATA ANALYSIS")
                println(
                  """
                    |Query Output
                    |""".stripMargin)
                if (input == "2" && SprintLog == "D") {

                  /*QUERY 4A SPARK SQL*/
                  val dfQ4a = sparkSession.sql(
                    "SELECT dayofweek(datetime), SUM(qty) " +
                      "FROM orders GROUP BY dayofweek(datetime)")
                    .toDF("Day", "Total Sold")
                    .sort("Total Sold")

                  /*QUERY 4A VEGAS VISUALIZATION*/
                  Vegas("Busiest days of week")
                    .withDataFrame(dfQ4a)
                    .mark(Bar)
                    .encodeX("Day", Nom)
                    .encodeY("Total Sold", Quant)
                    .show
                  dfQ4a.show()

                  /*QUERY 4A SPARK SQL*/
                  val dfQ4b = sparkSession.sql(
                    "SELECT country, dayofweek(datetime), SUM(qty) " +
                      "FROM orders GROUP BY country, dayofweek(datetime) LIMIT 6")
                    .toDF("Country","Day of Week", "Total Sold")
                    .sort("Country")

                  /*QUERY 4B VEGAS VISUALIZATION*/
                  Vegas("Busiest days of week by country")
                    .withDataFrame(dfQ4b)
                    .mark(Bar)
                    .encodeX("Country", Nom)
                    .encodeY("Total Sold", Quant)
                    .show
                  dfQ4b.show()
                  spark.stop()
                  println(
                    """
                      |4th Task Accomplished
                      |""".stripMargin)
                }
              case _ =>
                println("Wrong Entry, SprintLog In Session")
            }
          } while (SprintLog != "D")
          println("--" * 50)

        /*BLOCK 3 LOG OUT OPTION*/
        case "3" =>
          println("Log Out Initiated")

          /*NESTED DO WHILE LOOP WITHIN A DO WHILE LOOP
          TO FILTER LOG OUT OPTIONS*/
          do {
            print("To Log Out Type [Y]: ")
            LogOut = readLine
            LogOut match {
              case "Y" =>

                /*IF CONDITIONAL STATEMENT NESTED IN DO WHILE LOOP
                TO CREATE FUNCTIONAL LOG IN OPTIONS*/
                if (input == "3" && "Y" == LogOut) {
                  println(
                    """
                      |Log out SuccessFul
                      |
                      |Justin Yeary Is Offline 📴
                      |Kiara Gardea Is Offline 📴
                      |Trathen Mecimore Is Offline 📴
                      |Nicholas Famoye is Offline 📴
                      |""".stripMargin)
                }
              case _ =>
                println("Wrong Entry, Log Out Initiated!!")
            }
          } while (LogOut != "Y")
          println("--" * 50)
        case _ =>
          println("Wrong Entry, Choose Another Option [1][2][3]: ")
          println("--" * 50)
      }
    } while (input != "3")
  }
}





