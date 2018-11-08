package com.niuniuzcd.demo.trainAtom

import org.apache.spark.sql.DataFrame

object TrainUnits {
  type UNITS = DataFrame => DataFrame

  case class FlowMethod(name: String, func: UNITS)
  case class FlowUnit(name: String, obj: TrainProtocol[String])

   val filterObj: String => Filter = (str: String) => new Filter(str)
   val spliterObj: String => Spliter = (str: String) => new Spliter(str)
   val trainerObj: String => Trainer = (str: String) => new Trainer(str)
   val featureTransformObj: String => FeatureTransform = (str: String) => new FeatureTransform(str)

  object TrainComponents{
    abstract class Component(val name: String){
      type Value
    }
  }

  import TrainComponents.Component
  class Components{
    var data = Map.empty[Component, Any]
    def get(c: Component): Option[c.Value] = data.get(c).asInstanceOf[Option[c.Value]]
    def set(c:Component)(v: c.Value): Map[Component, Any] = {
      data = data.updated(c, v)
      data
    }
  }

  trait  Filters extends Component{
     type Value = Filter
  }

  trait  Spliters extends Component{
    type Value = Spliter
  }

  trait  Trainers extends Component{
    type Value = Trainer
  }

  trait  FeatureTransforms extends Component{
    type Value = FeatureTransform
  }

  object CC{
    val filter = new Component("TRAIN_FILTER") with Filters
    val spliter = new Component("FEA_SAMPLE_SPLIT") with Spliters
    val trainer = new Component("TRAIN_TRAINER") with Trainers
    val featureTransform = new Component("FEA_TRANSFORM") with FeatureTransforms
  }

}
