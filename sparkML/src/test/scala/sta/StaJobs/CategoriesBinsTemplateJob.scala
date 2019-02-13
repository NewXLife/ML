package sta.StaJobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf}
import sta.StaFlow

object CategoriesBinsTemplateJob extends Serializable {
  def useTemplateSta(row2ColsDf: DataFrame,
                     cMap: Map[String, Array[String]],
                     label:String = "label",
                     staTimeRange: String) = {
    import row2ColsDf.sparkSession.implicits._
    val staBinsDf = StaFlow.useBinsCategoriesTemplate(row2ColsDf, cMap).withColumn("bin_count", udf{ x: Seq[String]=> {
      x.size
    }}.apply(col("bins")))
    println("---------staBinsDf-----------")
    staBinsDf.show()

    val staDf = staBinsDf.withColumn("bin", StaFlow.categoriesDefinedBin($"value", $"bins"))
    println("-----------staDF------------")
    staDf.show()

    val binsDF = StaFlow.binsIndexExcludeMinMaxDF(staDf)
    println("-----------binsDF------------")
    binsDF.show()


    val masterDF  = StaFlow.totalIndexDF(row2ColsDf)
    println("-----------masterDF------------")
    masterDF.show()

    val binsIndex = StaFlow.binsExcludeMinMaxIndex(binsDF, masterDF)
    println("-----------binsIndex------------")
    binsIndex.show(100, truncate = 0)
    val totalIndex = StaFlow.totalCategoriesIndex(binsIndex)
    val finalDF = StaFlow.useBinsCategoriesTemplate(totalIndex, cMap).withColumn("bins", udf{ x: Seq[String]=> {
      x.toArray.mkString(",")
    }}.apply(col("bins")))

    println("-----------finalDF------------")
    finalDF.show()
  }
}
