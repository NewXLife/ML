package com.niuniuzcd.demo.ml.trainAtom

import org.apache.spark.sql.DataFrame

private[trainAtom] trait Train extends Serializable{
  object TrainComponents {
    abstract class Component(val name: String) {
      type Value
    }
  }

  import TrainComponents.Component
  class Components {
    var data = Map.empty[Component, Any]
    def get(c: Component): Option[c.Value] = data.get(c).asInstanceOf[Option[c.Value]]
    def set(c: Component)(v: c.Value): Map[Component, Any] = {
      data = data.updated(c, v)
      data
    }
  }

  object TrainModels {
    abstract class Model(val name: String) {
      type Value
    }
  }

  import TrainModels.Model
  class Models {
    var data = Map.empty[Model, Any]
    def get(c: Model): Option[c.Value] = data.get(c).asInstanceOf[Option[c.Value]]
    def set(c: Model)(v: c.Value): Map[Model, Any] = {
      data = data.updated(c, v)
      data
    }
  }
}
