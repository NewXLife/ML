package com.niuniuzcd.demo.ml.evaluator

object ModelSelection extends App {
  object Model extends Enumeration{
    type Model = Value
    val XGBClassifier,LinearRegression,LogisticRegression,DecisionTreeClassifier,KMeans = Value
  }
}
