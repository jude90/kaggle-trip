package my.kaggle

/**
 * Created by jude on 15-12-4.
 */

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.GradientBoostedTrees

import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.model.RandomForestModel

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.Vectors
import util.hashing.MurmurHash3


object Yummy {
  def main(args: Array[String]) {
      val sconf = new SparkConf().setAppName("yummy").setMaster("local[*]")
      val sc = new SparkContext(sconf)
      val data_set = sc.textFile("data/yummy/extract.csv")
      var NameDict = Map[String,Int]()

    val labled = data_set.map{ line =>
        val fields = line.split(",")
        val name = MurmurHash3.stringHash(fields.head)
        NameDict = NameDict.updated(fields.head, name)
        val sparse = fields.tail.map{ item  =>
          (MurmurHash3.stringHash(item) & Integer.MAX_VALUE , 1.toDouble)
        }.sortBy(_._1)
        LabeledPoint(name, Vectors.sparse(Integer.MAX_VALUE , sparse))
      }
    labled.count()

    println(NameDict.keys.toList.length)
  }

}
