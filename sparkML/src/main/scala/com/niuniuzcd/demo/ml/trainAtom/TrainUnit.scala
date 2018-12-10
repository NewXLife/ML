package com.niuniuzcd.demo.ml.trainAtom

import org.apache.spark.sql.DataFrame

object TrainUnit extends Train {
  type UNITS = DataFrame => DataFrame

  case class FlowMethod(name: String, func: UNITS)
  case class FlowUnit(name: String, obj: TrainProtocol[String])

   val filterObj: String => Filter = (str: String) => new Filter(str)
   val spliterObj: String => Spliter = (str: String) => new Spliter(str)
   val trainerObj: String => Trainer = (str: String) => new Trainer(str)
   val featureTransformObj: String => FeatureTransform = (str: String) => new FeatureTransform(str)


  trait  Filters extends TrainComponents.Component{
     type Value = Filter
  }

  trait  Spliters extends TrainComponents.Component{
    type Value = Spliter
  }

  trait  Trainers extends TrainComponents.Component{
    type Value = Trainer
  }

  trait  FeatureTransforms extends TrainComponents.Component{
    type Value = FeatureTransform
  }

  object TrainComponent{
    val filter = new TrainComponents.Component("TRAIN_FILTER") with Filters
    val spliter = new TrainComponents.Component("FEA_SAMPLE_SPLIT") with Spliters
    val trainer = new TrainComponents.Component("TRAIN_TRAINER") with Trainers
    val featureTransform = new TrainComponents.Component("FEA_TRANSFORM") with FeatureTransforms
  }

}
