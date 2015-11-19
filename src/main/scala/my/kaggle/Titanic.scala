package my.kaggle

/**
 * Created by jude on 2015/11/15.
 */

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.RandomForest

import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.model.RandomForestModel

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.Vectors
case class Passenger(pid:Int,survived:Int,
                     pclass:Int,name:String,
                     Sex:String,Age:String,
                     sibsp:Int,parch:Int,
                     ticket:String, fare:Double,
                     cabin:String,embarked:String)

case class Predcit(pid:Int,survived:Int)

object Titanic {
    val sconf = new SparkConf().setAppName("Titanic-survive").setMaster("local[*]")
    val sc = new SparkContext(sconf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def main(args: Array[String]) {
        val raw_train = sc.textFile("train.csv").cache()
        val passenger = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load("train.csv")
        val tests = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load("test.csv")
//      passenger.describe("Age").show()
//      passenger.describe("Cabin").show()
//      passenger
//      passenger.groupBy($"Fare").count().show()
//      passenger.groupBy($"Embarked").count().show()
//      passenger.groupBy($"Survived",$"Pclass").count().show()
//      passenger.groupBy($"Survived",$"Cabin").count().show()
//      passenger.groupBy($"Survived",$"Embarked").count().show()
//      passenger.groupBy($"Survived",$"Embarked").count().show()
//      passenger.groupBy($"Survived",$"Sex").count().show()
//  PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
    val labels =   passenger.map{row :Row =>

    val survived = row.getAs[Int]("Survived").toDouble

    val pclass  = row.getAs[Int]("Pclass").toDouble
    val age = row.getAs[Double]("Age")
    val sibsp = row.getAs[Int]("SibSp").toDouble
    val parch = row.getAs[Int]("Parch").toDouble
    val fare = row.getAs[Double]("Fare")
    val sex_cl:Double = row.getAs[String]("Sex") match {
          case "male" => 0
          case "female" =>1
        }
    val embarked_cl:Double = row.getAs[String]("Embarked") match {
          case "Q" => 1
          case "S" => 2
          case "C" => 3
          case _ => 0
        }

        LabeledPoint(survived.asInstanceOf[Int].toDouble,
          Vectors.dense(Array(pclass,sex_cl ,age, sibsp, parch,fare))
          )

      }

      val testfeature = tests.map{ row :Row =>

//          val survived = row.getAs[Double]("Survived")
        val pid = row.getAs[Int]("PassengerId")
        val pclass  = row.getAs[Int]("Pclass").toDouble
        val age = row.getAs[Double]("Age")
        val sibsp = row.getAs[Int]("SibSp").toDouble
        val parch = row.getAs[Int]("Parch").toDouble
          val fare = row.getAs[Double]("Fare")
          val sex_cl:Double = row.getAs[String]("Sex") match {
            case "male" => 0
            case "female" =>1
          }
          val embarked_cl:Double = row.getAs[String]("Embarked") match {
            case "Q" => 1
            case "S" => 2
            case "C" => 3
            case _ => 0
          }
        (pid, Vectors.dense(Array(pclass,sex_cl ,age, sibsp, parch,fare)))

      }
      // Train a DecisionTree model.
      //  Empty categoricalFeaturesInfo indicates all features are continuous.
      val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]()
      val impurity = "entropy"
      val maxDepth = 10
      val maxBins = 64
      val featureSubsetStrategy = "auto"
      val numTrees = 5
      val treemodel = RandomForest.trainClassifier(labels,2,
                      categoricalFeaturesInfo,numTrees,
        featureSubsetStrategy,impurity,maxDepth, maxBins)
      val labelpredicts = labels.map{ case LabeledPoint(label, features) =>
        val predict = treemodel.predict(features)
        (label, predict)
      }

      val trainErr = labelpredicts.filter{case(label, predict) =>
        label != predict
      }.count().toDouble / labelpredicts.count()
      println("Test Error = " + trainErr)

      val testpredicts = testfeature.map{ case(pid,feature) =>
        (pid, treemodel.predict(feature).toInt)
      }
      testpredicts.coalesce(1).saveAsTextFile("predicts.csv")
//      testpredicts.foreach(print)
      println("Learned classification tree model:\n" + treemodel.toDebugString)



      //      passenger.select("Embarked").distinct.foreach{ row:Row => println(row.getString(0))}
      }









//    def clean_data(line :String): LabeledPoint ={
//        survival        Survival
//                        (0 = No; 1 = Yes)
//        pclass          Passenger Class
//                        (1 = 1st; 2 = 2nd; 3 = 3rd)
//        name            Name
//        sex             Sex
//        age             Age
//        sibsp           Number of Siblings/Spouses Aboard
//        parch           Number of Parents/Children Aboard
//        ticket          Ticket Number
//        fare            Passenger Fare
//        cabin           Cabin
//          embarked        Port of Embarkation
//          (C = Cherbourg; Q = Queenstown; S = Southampton)

//        val field :Seq[String] = line.split(",")

//    }
}
