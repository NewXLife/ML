package ml

import java.util.Random

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, RowMatrix}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object SparkVectorTest extends App {
  //Breeze :是机器学习和数值技术库 ，它是sparkMlib的核心，包括线性代数、数值技术和优化，是一种通用、功能强大、有效的机器学习方法。
  //Epic     :是一种高性能能统计分析器和结构化预测库
  //Puck    :是一个快速GPU加速解析器

  /** 本地向量
    * spark 稠密向量和稀疏向量
    * 注意默认会引用mutable.vector它是一个容器，这里需要引用ml.linalg.{Vector, Vectors}
    * 一个向量的表示方法有两种
    * 例如: (1.0, 0.0 ,2.0)  稠密直接用数组表示(1.0, 0.0 ,2.0)， 稀疏表示：(向量大小，向量索引数组， 向量值）
    * 稀疏(存储元素的个数、以及非零元素的编号index和值value)： (3, Array(0,2), Array(1.0, 2.0))
    * 以上就是他们的的区别
    */

  import org.apache.spark.ml.linalg.{Vector, Vectors}

  //Vectors是vector 的工厂
  val sarkDenseVector = Vectors.dense(Array(1.0, 0.0, 2.0))
  val sparkVector = Vectors.sparse(3, Array(0, 1), Array(1.0, 2.0))
  sparkVector.numActives // inactive entries have value 0.
  sparkVector.argmax

  //Returns a vector in either dense or sparse format,
  // whichever uses less storage.
  // A dense vector needs 8 * size + 8 bytes,
  // while a sparse vector needs 12 * nnz(numNonzeros) + 20 bytes.
  sparkVector.compressed
  sarkDenseVector.compressed

  sparkVector.hashCode()
  sparkVector.numNonzeros
  sparkVector.size
  sparkVector.toSparse

  //索引访问
  sparkVector(0)

  // scala.collection.immutable.Vector => ml.linalg.Vector
  val testScalaVector = Seq(1, 2, 3).map(x => x.toDouble).to[scala.Vector].toArray

  val testScalaVector1 = Seq[Double](1, 2, 3).to[scala.Vector].toArray
  Vectors.dense(testScalaVector1)

  val ddd = Array(1.0, 20.0, 3.0)
  val scalaVec = scala.Vector(1.0, 2.0, 3.0)
  val sparkVec = Vectors.dense(ddd)
  val sparkVec1 = Vectors.dense(1.0, ddd: _*)


  import org.apache.spark.ml.linalg.{SparseVector => MLSparseVector}
  import org.apache.spark.ml.linalg.{DenseVector => MLDenseVector}

  val mlDenseVector = new MLDenseVector(Array(1, 0, 0, 0, 0))
  mlDenseVector.toSparse
  val mlsparkVector = new MLSparseVector(5, Array(0), Array(1))

  // mllib中的vector转换为 ml中的vector  =》  asML
  import org.apache.spark.mllib.linalg.{DenseVector => MLLibDenseVector}

  val mLLibDenseVector = new MLLibDenseVector(Array[Double](1, 0, 1))
  mLLibDenseVector.asML

  /**
    * label Vector
    * 向量标签和向量是一起的，简单来说，可以理解为一个向量对应的一个特殊值(向量标识）
    * 你可以把向量标签作为行索引，从而用多个本地向量构成一个矩阵（当然，MLlib中已经实现了多种矩阵）
    */

  //import org.apache.spark.ml.feature.LabeledPoint
  val labelVector = LabeledPoint(1.0, Vectors.dense(1.0, 2.0, 3.0))


  /**
    * 本地矩阵
    */

  import org.apache.spark.ml.linalg.{Matrix, Matrices}

  val localMatrix = Matrices.dense(3, 2, Array(1, 2, 3, 4, 5, 6))

  import org.apache.spark.ml.linalg.{DenseMatrix => MLDenseMatrix}
  import org.apache.spark.ml.linalg.{SparseMatrix => MLSparseMatrix}

  // 第三个参数为是否允许转至，默认不允许，如果允许则按行存储
  val localMatrix1 = new MLDenseMatrix(3, 2, Array(1, 2, 3, 4, 5, 6), true)
  localMatrix1.isTransposed
  localMatrix1.toSparse

  MLDenseMatrix.zeros(3, 3)
  MLDenseMatrix.ones(3, 2)

  //单位阵
  MLDenseMatrix.eye(3)

  //对角矩阵，对角元素为vector值
  MLDenseMatrix.diag(Vectors.dense(1, 2, 3))
  MLDenseMatrix.rand(3, 2, new Random(10))

  // sparse matrix 稀疏矩阵
  /**
    * Column-major sparse matrix.
    * The entry values are stored in Compressed Sparse Column (CSC) format.
    * For example, the following matrix
    * {{{
    *   1.0 0.0 4.0
    *   0.0 3.0 5.0
    *   2.0 0.0 6.0
    * }}}
    * is stored as `values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
    * `rowIndices=[0, 2, 1, 0, 1, 2]` => 确定每个元素所在的行,  即 1.0 在0行， 2.0 在第二行，3.0 在第一行...
    * `colPointers=[0, 2, 3, 6]` => (2-0,3-2,6-3 )得到每一列非零元素个数, 即第一列非0元素个数是 2-0 是2个，第二列非0 是3-2 = 1个...
    **/
  //  def this(numRows: Int,numCols: Int,colPtrs: Array[Int],rowIndices: Array[Int],values: Array[Double]) = this(numRows, numCols, colPtrs, rowIndices, values, false)
  // colPtrs the index corresponding to the start of a new column 与新列开始相对应的索引
  val localSparkMatrix = new MLSparseMatrix(3, 3, Array[Int](0, 2, 3, 6), Array[Int](0, 2, 1, 0, 1, 2), Array[Double](1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
  val localSparkMatrix1 = Matrices.sparse(3, 3, Array[Int](0, 2, 3, 6), Array[Int](0, 2, 1, 0, 1, 2), Array[Double](1.0, 2.0, 3.0, 4.0, 5.0, 6.0))

  /**
    * 分布式矩阵
    * BlockMatrix
    * RowMatrix
    * IndexedRowMatrix ，有行索引，可以通过索引值来访问每一行
    * CoordinateMatrix ，数据特别稀疏的时候用
    * 预先知道用什么样的矩阵是比较好的，如果在使用分布式矩阵的过程中转换矩阵类型，数据量比较大的时候，开销比较大（转换的时候数据要重新shuffle）
    */
  // BlockMatrix
  // 是处理阶数较高的矩阵时常采用的技巧，也是数学在多领域的研究工具。
  // 对矩阵进行适当分块，可使高阶矩阵的运算可以转化为低阶矩阵的运算，
  // 同时也使原矩阵的结构显得简单而清晰，从而能够大大简化运算步骤


  import org.apache.spark.mllib.linalg.{Matrix, Vector => MLLIbVector, Vectors => MlLibVectors}

  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  val rowMatrixRdd: RDD[MLLIbVector] = spark.sparkContext.parallelize(
    Array(
      Array[Double](1, 2, 3, 4),
      Array[Double](1, 2, 3, 4),
      Array[Double](1, 2, 3, 4)
    )
  ).map(f => MlLibVectors.dense(f))

  val mat = new RowMatrix(rowMatrixRdd)

  val coordinateMatrixRdd = spark.sparkContext.parallelize(
    Array(
      Array[Double](1, 2, 3, 4),
      Array[Double](1, 2, 3, 4),
      Array[Double](1, 2, 3, 4)
    )
  )
//  val corMat = new CoordinateMatrix(rdd1)
//  mat.numCols()
//  mat.numRows()
//  val summarl = mat.computeColumnSummaryStatistics()



  // Spark MLlib Statistics统计 目前只包含下面的指标
  // 计算每列最大值、最小值、平均值、方差值、L1范数、L2范数。
  val sta1 = Statistics.colStats(rowMatrixRdd)
  sta1.max
  sta1.min
  sta1.mean
  sta1.variance
  sta1.normL1
  sta1.normL2
  sta1.numNonzeros

  // 相关系数， 默认求的是 pearson 皮尔森系数， spearman
  val corr1 = Statistics.corr(rowMatrixRdd)
  val corr2 = Statistics.corr(rowMatrixRdd, "pearson")
  val corr3 = Statistics.corr(rowMatrixRdd, "spearman")

  val corrRdd1 =  spark.sparkContext.parallelize(Array[Double](1,2,3))
  val corrRdd2 =  spark.sparkContext.parallelize(Array[Double](1,2,3))

  // 计算两个数据集的相关系数, 要求需要相同的分区和每个分区有相同的元素个数
  // The two input RDDs need to have the same number of partitions and the same number of elements in each partition.
  val corr4 = Statistics.corr(corrRdd1, corrRdd2, "pearson")
}
