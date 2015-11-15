package my.kaggle

/**
 * Created by jude on 2015/11/15.
 */

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};


case class Passenger(pid:Int,survived:Int,
                     pclass:Int,name:String,
                     Sex:String,Age:String,
                     sibsp:Int,parch:Int,
                     ticket:String, fare:Double,
                     cabin:String,embarked:String)

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
//        val df = sc.textFile("train.csv").map(_.split(","))
//          .map{p =>
//            Passenger(p(0).toInt, p(1).toInt,
//                      p(2).toInt, p(3).trim,
//                      p(4).trim, p(5).trim,
//                      p(6).toInt, p(7).toInt,
//                      p(8).trim, p(9).toDouble,
//                      p(10).trim,p(11).trim)
//
//        }.toDF()

//        df.registerTempTable("titantic")
//        passenger.show()
      passenger.describe("Age").show()
      passenger.describe("Cabin")
     val freq =  passenger.stat.freqItems(Array("Cabin","Embarked"))
      freq.collect().foreach{ row =>
      println(row(0))
        println(row(1))
      }
        //        val fare_mean = raw_train.map { line =>
//            line.split(",")
//        }

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
