package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    if(isEmpty(queryRectangle) || isEmpty(pointString)) {
      return false
    }
    var rectArray = queryRectangle.split(",")
    var x1 = rectArray(0).toDouble
    var y1 = rectArray(1).toDouble
    var x2 = rectArray(2).toDouble
    var y2 = rectArray(3).toDouble

    val pointArray = pointString.split(",")
    var x = pointArray(0).toDouble
    var y = pointArray(1).toDouble

    // To ensure point lies within bottom (x1,y1) and top (x2,y2) or bottom (x2,y2) and top (x1,y1)
    return ((x >= x1 && x <= x2 && y >= y1 && y <= y2) || (x >= x2 && x <= x1 && y >= y2 && y <= y1))
  }

   // check if a String is empty or not
  def isEmpty(x: String) = x == null || x.trim.isEmpty
}
