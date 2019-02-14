package base

import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

object DataFrameExtensions {
  implicit def extendedDataFrame(dataFrame: DataFrame): ExtendedDataFrame =
    new ExtendedDataFrame(dataFrame: DataFrame)

  class ExtendedDataFrame(dataFrame: DataFrame) {
    def isEmpty: Boolean = {
      Try{dataFrame.head().length != 0} match {
        case Success(_) => false
        case Failure(_) => true
      }
    }

    def nonEmpty(): Boolean = !isEmpty
  }
}
