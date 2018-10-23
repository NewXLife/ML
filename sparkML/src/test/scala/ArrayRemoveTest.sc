import scala.collection.mutable.ArrayBuffer

val baseFeatureList = Array("d14","apply_date","7day","1month","3month","6month","12month","18month","24month","60month")
baseFeatureList.indexOf("d14")
baseFeatureList.indexOf("apply_date")
val t = baseFeatureList.toBuffer.remove(0)
baseFeatureList

val temp  = ArrayBuffer("d14","apply_date","7day","1month","3month","6month","12month","18month","24month","60month")
temp.indexOf("d14")
temp.indexOf("apply_date")

temp.remove(temp.indexOf("d14"))
temp.remove(temp.indexOf("apply_date"))
temp
