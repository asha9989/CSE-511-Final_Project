package cse512

import org.apache.spark.sql.SparkSession
import scala.math.pow
import scala.math.sqrt

object SpatialQuery extends App {
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Contains",
      (queryRectangle: String, pointString: String) => {
        ST_Contains(queryRectangle, pointString)
      }
    )

    val resultDf = spark.sql(
      "select * from point where ST_Contains('" + arg2 + "',point._c0)"
    )
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(
      spark: SparkSession,
      arg1: String,
      arg2: String
  ): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Contains",
      (queryRectangle: String, pointString: String) => {
        ST_Contains(queryRectangle, pointString)
      }
    )

    val resultDf = spark.sql(
      "select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)"
    )
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(
      spark: SparkSession,
      arg1: String,
      arg2: String,
      arg3: String
  ): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Within",
      (pointString1: String, pointString2: String, distance: Double) => {
        ST_Within(pointString1, pointString2, distance)
      }
    )

    val resultDf = spark.sql(
      "select * from point where ST_Within(point._c0,'" + arg2 + "'," + arg3 + ")"
    )
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(
      spark: SparkSession,
      arg1: String,
      arg2: String,
      arg3: String
  ): Long = {

    val pointDf = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register(
      "ST_Within",
      (pointString1: String, pointString2: String, distance: Double) => {
        ST_Within(pointString1, pointString2, distance)
      }
    )
    val resultDf = spark.sql(
      "select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, " + arg3 + ")"
    )
    resultDf.show()

    return resultDf.count()
  }

  /*ST_Contains function is used to find whether the queryRectangle fully contains the point. To check this, x - coordinate of the point should be within the x- coordinates
  of rectangle. Similarly y- coordinates of the point should be within the y- coordinates of the rectangle.
  output: boolean
  input (pointString:String, queryRectangle:String):  pointString (comma separated -> coordinates of the point), queryRectangle (comma separated -> top and bottom coordinates of the rectangle)
   */
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    if (isEmpty(queryRectangle) || isEmpty(pointString)) {
      return false
    }

    var rectArr = queryRectangle.split(",")
    var x1 = rectArr(0).toDouble
    var y1 = rectArr(1).toDouble
    var x2 = rectArr(2).toDouble
    var y2 = rectArr(3).toDouble

    val pointArr = pointString.split(",")
    var x = pointArr(0).toDouble
    var y = pointArr(1).toDouble

    // To ensure point lies within bottom (x1,y1) and top (x2,y2) or bottom (x2,y2) and top (x1,y1)

    return ((x >= x1 && x <= x2 && y >= y1 && y <= y2) || (x >= x2 && x <= x1 && y >= y2 && y <= y1))
  }

  /*ST_Within function is used to find whether the two points are within the given distance( i.e. Euclidean distance).
    output: boolean
    input (pointString1:String, pointString2:String, distance:Double):  pointString1 (comma separated -> coordinates of the point), pointString2 (comma separated -> coordinates of the point), distance to check if two points are within this distance.
   */
  def ST_Within(
      pointString1: String,
      pointString2: String,
      distance: Double
  ): Boolean = {
    if (isEmpty(pointString1) || isEmpty(pointString2) || distance <= 0.00) {
      return false
    }
    var pointArr1 = pointString1.split(",")
    var x1 = pointArr1(0).toDouble
    var y1 = pointArr1(1).toDouble

    var pointArr2 = pointString2.split(",")
    var x2 = pointArr2(0).toDouble
    var y2 = pointArr2(1).toDouble

    // Euclidean distance between two points.
    var euclideanDistance =
      sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))

    return euclideanDistance <= distance
  }

  // check if a String is empty or not
  def isEmpty(x: String) = x == null || x.trim.isEmpty
}
