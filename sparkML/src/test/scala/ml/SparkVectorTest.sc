import java.util.Random

import org.apache.spark.ml.linalg.{Vector, Vectors}


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

val test = Seq(1,2,3).map(x => x.toDouble).to[scala.Vector].toArray
Vectors.dense(test)

import org.apache.spark.ml.linalg.{SparseVector => MLSparseVector}
import org.apache.spark.ml.linalg.{DenseVector => MLDenseVector}
val mlDenseVector = new MLDenseVector(Array(1, 0, 0, 0, 0))
mlDenseVector.toSparse
val mlsparkVector = new MLSparseVector(5,Array(0), Array(1))

val t = Array[Double](1)
val testScalaVector1 = Seq[Double](1,2,3).to[scala.Vector].toArray
Vectors.dense(testScalaVector1)

//matrix
import org.apache.spark.ml.linalg.{Matrix, Matrices}
val localMatrix = Matrices.dense(3, 2, Array(1,2,3,0,5,6))
localMatrix.isTransposed
localMatrix.numActives
localMatrix.numCols
localMatrix.numNonzeros
localMatrix.numRows
//索引访问
localMatrix(1,1)

import org.apache.spark.ml.linalg.{DenseMatrix => MLDenseMatrix}
import org.apache.spark.ml.linalg.{SparseMatrix => MLSparseMatrix}
// 第三个参数为是否允许转至，默认不允许，如果允许则按行存储
val localMatrix1 = new MLDenseMatrix(3, 2, Array(1,2,3,4,5,6), true)
localMatrix1.isTransposed
localMatrix1.toSparse

MLDenseMatrix.zeros(3,3)
MLDenseMatrix.ones(3,2)

//单位阵
MLDenseMatrix.eye(3)

//对角矩阵，对角元素为vector值
MLDenseMatrix.diag(Vectors.dense(1,2,3))
MLDenseMatrix.rand(3,2,new Random(10))

val localSparkMatrix = new MLSparseMatrix(3, 3, Array[Int](0, 2, 3, 6), Array[Int](0, 2, 1, 0, 1, 2), Array[Double](1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
localSparkMatrix.toDense
val localSparkMatrix1 = Matrices.sparse(3, 3, Array[Int](0, 2, 3, 6), Array[Int](0, 2, 1, 0, 1, 2), Array[Double](1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
