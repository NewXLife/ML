package org.apache.spark.ml.tree

import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.param.DoubleArrayParam

import scala.collection.mutable
object DTUtils {
  /**
    * Extract path
    *
    * @param model
    */
  def extractPath(model: DecisionTreeClassificationModel): DoubleArrayParam = {
    model.thresholds
  }

  /**
    * Extract con bins
    *
    * @param model
    * @return
    */
  def extractConBins(model: DecisionTreeClassificationModel): Array[Double] = {
    val splits = getSplits(model)
    val points = splits.map {
      case s: ContinuousSplit =>
        val point: Double = s.threshold
        point
      case one: Split =>
        println(s"type not constant+${one.featureIndex}")
        -100.0d
    }
    (Double.NegativeInfinity +: points.sorted :+ Double.PositiveInfinity).toArray
  }

  /**
    * Extract cate bins
    *
    * @param model
    * @return
    */
  def extractCateBins(model: DecisionTreeClassificationModel): Array[Double] = {
    val splits = getSplits(model)
    val points = splits.map {
      case s: CategoricalSplit =>
        s.leftCategories
      case one: Split =>
        println(s"type not constant+${one.featureIndex}")
        Array(-100.0d)
    }
    points.flatten.sorted.toArray
  }


  val init = (Double.MaxValue, null.asInstanceOf[Array[Double]], Double.MinValue)

  def extractBinInfo(model: DecisionTreeClassificationModel): mutable.Buffer[BinInfo] = {
    val binInfos: mutable.Seq[Either[Split, Array[Double]]] = getInfo(model)
    val bins = mutable.Buffer[BinInfo]()

    var tmp: BinTmpInfo = tupleToBin(init)

    val iter = binInfos.iterator
    while (iter.hasNext) {
      // handle init
      if (tmp.top > tmp.bot) {
        val nb = tmp.top
        bins.append(BinInfo((tmp.bot, tmp.top), tmp.leaf))
        tmp = tupleToBin(init)
        tmp.bot = nb
      }

      val one = iter.next()
      one match {
        case Left(l) =>
          val sp = l match {
            case s: ContinuousSplit => {
              val point: Double = s.threshold
              point
            }
          }
          if (tmp.bot == Double.MaxValue) {
            tmp.bot = sp
          } else if (tmp.top == Double.MinValue) {
            tmp.top = sp
          }
        case Right(r) =>
          tmp.leaf = r
          if (tmp.bot == Double.MaxValue) {
            tmp.bot = Double.MinValue
          }
      }
    }

    if (tmp.leaf != null) {
      if (tmp.top == Double.MinValue) {
        tmp.top = Double.MaxValue
      }
      bins.append(BinInfo((tmp.bot, tmp.top), tmp.leaf))
    }
    bins
  }

  def extractCateBinInfo(model: DecisionTreeClassificationModel): mutable.Buffer[BinInfo] = {
    val binInfos: mutable.Seq[Either[Split, Array[Double]]] = getInfo(model)
    val bins = mutable.Buffer[BinInfo]()

    var tmp: BinTmpInfo = tupleToBin(init)

    val iter = binInfos.iterator
    while (iter.hasNext) {
      // handle init
      if (tmp.top > tmp.bot) {
        val nb = tmp.top
        bins.append(BinInfo((tmp.bot, tmp.top), tmp.leaf))
        tmp = tupleToBin(init)
        tmp.bot = nb
      }

      val one = iter.next()
      one match {
        case Left(l) =>
          val sp = l match {
            case s: ContinuousSplit =>
              val point: Double = s.threshold
              point
          }
          if (tmp.bot == Double.MaxValue) {
            tmp.bot = sp
          } else if (tmp.top == Double.MinValue) {
            tmp.top = sp
          }
        case Right(r) =>
          tmp.leaf = r
          if (tmp.bot == Double.MaxValue) {
            tmp.bot = Double.MinValue
          }
      }
    }
    if (tmp.leaf != null) {
      if (tmp.top == Double.MinValue) {
        tmp.top = Double.MaxValue
      }
      bins.append(BinInfo((tmp.bot, tmp.top), tmp.leaf))
    }
    bins
  }


  //~----------------------------------------------------------------------

  /**
    * Extract splits
    *
    * @param model
    * @return
    */
  private[tree] def getSplits(model: DecisionTreeClassificationModel): mutable.Seq[Split] = {
    val buf = mutable.Buffer[Split]()
    recursiveExtraSplits(model.rootNode, buf)
    buf
  }

  /**
    *
    * @param node
    */
  private[tree] def recursiveExtraSplits(node: Node, buf: mutable.Buffer[Split]): Unit = {

    if (node.isInstanceOf[LeafNode]) {
      return
    }
    node match {
      case node: InternalNode =>
        val left = node.leftChild
        if (left != null) {
          recursiveExtraSplits(left, buf)
        }

        buf.append(node.split)

        val right = node.rightChild
        if (right != null) {
          recursiveExtraSplits(right, buf)
        }

      case leaf: LeafNode =>
        println(s"Can not recognized node type!+${leaf.getClass.toString}")
    }
  }


  /**
    * extract bins for feature
    *
    * @param model
    * @return
    */
  private[tree] def getInfo(model: DecisionTreeClassificationModel): mutable.Seq[Either[Split, Array[Double]]] = {
    val buf = mutable.Buffer[Either[Split, Array[Double]]]()
    recursiveExtraInfo(model.rootNode, buf)
    buf
  }


  /**
    * recursive  get ExtraInfo
    *
    * @param node
    */
  private[tree] def recursiveExtraInfo(node: Node, buf: mutable.Buffer[Either[Split, Array[Double]]]): Unit = {
    node match {
      case node: InternalNode =>
        val left = node.leftChild
        if (left != null) {
          recursiveExtraInfo(left, buf)
        }
        buf.append(Left(node.split))
        if (node.rightChild != null) {
          recursiveExtraInfo(node.rightChild, buf)
        }
      case leaf: LeafNode =>
        buf.append(Right(leaf.impurityStats.stats))
    }
  }

  case class BinTmpInfo(var bot: Double, var leaf: Array[Double], var top: Double)

  case class BinInfo(range: (Double, Double), hitInfo: Array[Double])

  implicit def tupleToBin(db: (Double, Array[Double], Double)): BinTmpInfo = BinTmpInfo(db._1, db._2, db._3)
}
