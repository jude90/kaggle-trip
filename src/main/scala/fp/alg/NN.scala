package fp.alg

/**
 * Created by jude on 15-12-18.
 */

import breeze.numerics.{pow, sqrt}
import org.apache.spark.rdd.RDD
class NN {
  def distance(xs: Array[Double], ys: Array[Double]) = {
    sqrt(( xs zip ys) map { case(x,y) => pow(y - x, 2)} sum)

  }
  def findNearestClasses(testPoints: RDD[Array[Double]],trainPoints: Array[Array[Double]]) ={

  }
}
