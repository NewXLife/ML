package base

import com.niuniuzcd.demo.ml.evaluator.ModelSelection

object TestClass extends App{


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
