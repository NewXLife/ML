package base

import com.niuniuzcd.demo.ml.evaluator.ModelSelection

import scala.collection.mutable.ArrayBuffer

object TestClass extends App{


  val featureArray = Array("f1", "f2", "f3")
  val totalCols = ArrayBuffer("f1", "f2", "f3","f4","label")

  var staCols = featureArray.toBuffer
  val label = "label"
  staCols = if(!staCols.exists(name => label.contains(name)))  label +: staCols else staCols


  println(staCols.mkString(","))

  if(staCols.exists(name => label.contains(name))) staCols.remove(staCols.indexOf(label))

  println(staCols.mkString(","))
  // model match
  ModelSelection.Model.withName("XGBClassifier") match {
    case ModelSelection.Model.XGBClassifier =>  println("xgbclassifier")
    case ModelSelection.Model.KMeans => println("KMeans")
    case ModelSelection.Model.DecisionTreeClassifier => println("DecisionTreeClassifier")
    case ModelSelection.Model.LinearRegression => println("LinearRegression")
    case ModelSelection.Model.LogisticRegression => println("LogisticRegression")
    case _ => println("")
  }
}
