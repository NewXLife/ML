package sta


import scala.collection.mutable.ArrayBuffer

object BinningCutTest extends App {

  def cut(min:Double, max:Double, bins:Int) ={
    var cut_points = cutMain(min, max, bins)
    cut_points = Double.NegativeInfinity +: cut_points
    cut_points = cut_points :+ Double.PositiveInfinity
    cut_points
  }

  def cutMain(min:Double, max:Double, bins:Int) = {
    val bin = bins + 1
    val right = true
    var mn = min
    var mx = max
    if (mn == mx) {
      mn = mn - (if (mn != 0).001 * math.abs(mn) else .001)
      mx = mx + (if (mx != 0).001 * math.abs(mx) else .001)
      //      bins = linspace(mn, mx, cut_num + 1)
      linspace2(mn, mx, bin).toBuffer
    } else {
      val bins = linspace2(mn, mx, bin).toBuffer
      val adj = (mx - mn) * 0.001
      if (right) bins(0) -= adj else bins(bins.length - 1) += adj
      bins
    }
  }


  def linspace(start: Double, stop: Double, num: Int = 10, endpoint: Boolean = true) = {
    require(num > 0, "Number of samples, %s, must be non-negative.")

    val div = if (endpoint) num - 1 else num

    val delta = stop * 1.0 - start * 1.0

    val d = delta / div

    val y = Range(0, div) // 0 until div

    if (num > 1) {
      val step = delta / (div - 1)
      if (step == 0) {
        val t = for (i <- y) yield i / div
        for (i <- t) yield i * delta
      } else {
        for (i <- y) yield i * step
      }
    } else {
      val step = None
      for (i <- y) yield i * delta
    }
  }

  def linspace2(start: Double, stop: Double, num: Int = 10, endpoint: Boolean = true) = {
    require(num > 0, "Number of samples, %s, must be non-negative.")

    val div = if (endpoint) num - 1 else num

    val delta = stop * 1.0 - start * 1.0
    val d = delta / div

    val ab = ArrayBuffer[Double]()
    var cut_point = start
    for (_ <- 0 to div) {
      ab += cut_point
      cut_point = cut_point + d
    }
    ab
  }

  println(cut(20, 61, 3).mkString(","))

}
