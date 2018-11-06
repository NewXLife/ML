package com.niuniuzcd.demo.trainAtom

import com.niuniuzcd.demo.util.JsonUtil

object TestTrainJob extends App {
  //[FEA_DS, FEA_SAMPLE_SPLIT, FEA_TRANSFORM, TRAIN_FILTER, TRAIN_TRAINER] 代表训练顺序从dag构建而来
  val inputStr = TestPro.getJsonSr
  val keySet = JsonUtil.getKeySet(inputStr)

  keySet.remove("FEA_DS")
}
