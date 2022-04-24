package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART

    pickupInfo.createOrReplaceTempView("pickupInfoView")
    pickupInfo = spark.sql("select x,y,z from pickupInfoView where x>= " + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x")
    pickupInfo.createOrReplaceTempView("selectedCells")

    pickupInfo = spark.sql("select x, y, z, count(*) as hotCells from selectedCells group by x, y, z order by z,y,x")
    pickupInfo.createOrReplaceTempView("selectedCellHotness")


    val selectedCellsCount = spark.sql("select sum(hotCells) as sumHotCells from selectedCellHotness")
    selectedCellsCount.createOrReplaceTempView("selectedCellsCount")

    val mean = (selectedCellsCount.first().getLong(0).toDouble / numCells.toDouble).toDouble

    spark.udf.register("squared", (inputX: Int) => (((inputX*inputX).toDouble)))

    val squareSum = spark.sql("select sum(squared(hotCells)) as squareSum from selectedCellHotness")
    squareSum.createOrReplaceTempView("squareSum")

    val sd = scala.math.sqrt(((squareSum.first().getDouble(0).toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))).toDouble

    spark.udf.register("neighbourCells", (inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.getNeighbourCells(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))))

    val neighbourCells = spark.sql("select neighbourCells(cord1.x, cord1.y, cord1.z, " + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") as neighbourCellCount,"
      + "cord1.x as x, cord1.y as y, cord1.z as z, "
      + "sum(cord2.hotCells) as sumHotCells "
      + "from selectedCellHotness as cord1, selectedCellHotness as cord2 "
      + "where (cord2.x = cord1.x+1 or cord2.x = cord1.x or cord2.x = cord1.x-1) "
      + "and (cord2.y = cord1.y+1 or cord2.y = cord1.y or cord2.y = cord1.y-1) "
      + "and (cord2.z = cord1.z+1 or cord2.z = cord1.z or cord2.z = cord1.z-1) "
      + "group by cord1.z, cord1.y, cord1.x "
      + "order by cord1.z, cord1.y, cord1.x")
    neighbourCells.createOrReplaceTempView("neighbourCells")

    spark.udf.register("getisOrd", (neighbourCellCount: Int, sumHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, sd: Double) => ((HotcellUtils.getGetisOrd(neighbourCellCount, sumHotCells, numCells, x, y, z, mean, sd))))

    pickupInfo = spark.sql("select getisOrd(neighbourCellCount, sumHotCells, "+ numCells + ", x, y, z," + mean + ", " + sd + ") as getisOrdStatistic, x, y, z from neighbourCells order by getisOrdStatistic desc");
    pickupInfo.createOrReplaceTempView("getisOrd")
    pickupInfo = spark.sql("select x, y, z from getisOrd")
    pickupInfo.createOrReplaceTempView("resultPickupInfo")
    return pickupInfo
  }
}