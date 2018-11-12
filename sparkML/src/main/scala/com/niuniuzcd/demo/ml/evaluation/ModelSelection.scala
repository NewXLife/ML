package com.niuniuzcd.demo.ml.evaluation

object ModelSelection{
  object Model extends Enumeration{
    type Model = Value
    val XGBClassifier,LinearRegression,LogisticRegression,DecisionTreeClassifier,KMeans = Value
  }
}
