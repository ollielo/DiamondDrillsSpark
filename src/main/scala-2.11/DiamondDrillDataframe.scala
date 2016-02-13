import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

/**
  * Created by ollie on 2/11/16.
  */

case class Diamond(carat: Double, cut: String, color: String, clarity: String,
                   depth: Double, table: Double, price: Int,
                   x: Double, y: Double, z: Double)

object DiamondDrillDataframe extends App {
  val conf = new SparkConf().setAppName("Diamond Dripp Dataframe")
    .setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val diamonds =  sc.textFile("src/main/resources/diamonds.csv")
    .map(_.replace("\"", ""))
    .map(_.split(","))
    .map(fields => Diamond(fields(0).toDouble, fields(1), fields(2), fields(3),
      fields(4).toDouble, fields(5).toDouble, fields(6).toInt,
      fields(7).toDouble, fields(8).toDouble, fields(9).toDouble))
    .toDF()

  diamonds.show()

  // Problem 1.
  // Make a new data set that has the average depth and price of the diamonds
  // in the data set.
  //diamonds.select(expr("avg(price)"), expr("avg(depth)")).show()
  diamonds.select(avg($"price"), avg($"depth")).show()

  // Problem 2.
  // Add a new column to the data set that records each diamond's price per carat.
  diamonds.select(expr("*"), expr("price/carat")).show()

  // Problem 3.
  // Make a data set that only includes diamonds with an Ideal cut.
  diamonds.filter($"cut" === "Ideal").show()

  // Problem 4.
  // Create a new data set that groups diamonds by their cut and displays the
  // average price of each group.
  diamonds.groupBy($"cut").mean("price").show()

  // Problem 5.
  // Create a new data set that groups diamonds by color and displays the
  // average depth and average table for each group.
  diamonds.groupBy($"color").mean("depth", "table").show()

  // Problem 6.
  // Add two columns to the diamonds data set. The first column should display
  // the average depth of diamonds in the diamond's color group. The second
  // column should display the average table of diamonds in the diamonds color
  // group.
  //diamonds.join(diamonds.groupBy($"color").mean("depth", "table")).show()

  // Problem 7.
  // Make a data set that contains all of the unique combinations of cut, color,
  // and clarity, as well the average price of diamonds in each group.
  diamonds.groupBy($"cut", $"color", $"clarity").agg(mean($"price")).show()

  // Problem 8.
  // Add a column to the diamonds data set that displays the average price for
  // all diamonds with a diamond's cut, color, and clarity


  // Problem 9.
  // Do diamonds with the best cut fetch the best price for a given amount of carats?
  diamonds.groupBy($"cut").agg(avg($"price"/$"carat")).show()

  // Problem 10.
  // Which color diamonds seem to be largest on average (in terms of carats)?
  diamonds.groupBy($"color").mean("carat").orderBy("avg(carat)").show()

  // Problem 11.
  // What color of diamonds occurs the most frequently among diamonds with ideal cuts?
  diamonds.filter($"cut" === "Ideal").groupBy($"color").count().show()

  // Problem 12.
  // Which clarity of diamonds has the largest average table per carats?
  diamonds.groupBy($"clarity").agg(avg($"table"/$"carat")).show()

  // Problem 13.
  // Which diamond has the largest price per carat compared other diamonds with
  // its cut, color, and clarity?

  // Problem 14.
  // What is the average price per carat of diamonds that cost more than $10000?
  diamonds.filter($"price" > 10000).agg(avg($"price"/$"carat")).show()

  // Problem 15.
  // Display the largest diamond depth observed for each clarity group.
  diamonds.groupBy($"clarity").max("depth").show()
}
