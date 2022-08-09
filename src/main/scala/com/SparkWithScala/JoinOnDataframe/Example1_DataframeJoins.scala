package com.SparkWithScala.JoinOnDataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Example1_DataframeJoins {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession.builder()
          .appName("All the joins")
          .master("local[2]")
          .getOrCreate()

        val sc = spark.sparkContext   // for creating  RDD through parallelize

        val kids = sc.parallelize(List(
          Row(40, "Mary", 1),
          Row(41, "Jane", 3),
          Row(42, "David", 3),
          Row(43, "Angela", 2),
          Row(44, "Charlie", 1),
          Row(45, "Jimmy", 2),
          Row(46, "Lonely", 7)
        ))

        val kidsSchema = StructType(Array(
          StructField("Id",IntegerType),
          StructField("Name",StringType),
          StructField("Team",IntegerType),
        ))

        val kidsDF = spark.createDataFrame(kids,kidsSchema)

        val teams = sc.parallelize(List(
            Row(1, "The Invincibles"),
            Row(2, "Dog Lovers"),
            Row(3, "Rockstars"),
            Row(4, "The Non-Existent Team"),
        ))

        val teamsSchema = StructType(Array(
          StructField("TeamId",IntegerType),
          StructField("TeamName",StringType)
        ))

        val teamsDF = spark.createDataFrame(teams,teamsSchema)

        kidsDF.show()
        teamsDF.show()


      // Join Type 1 : Inner Joins

      val joinCondition = kidsDF.col("Team") === teamsDF.col("TeamId")
      val kidsTeamsDF = kidsDF.join(teamsDF,joinCondition,"inner")

      kidsTeamsDF.show()

      // Join Type 2 : Cross Join

      kidsDF.crossJoin(teamsDF)  //cross join it will be cross product

      // Join Type 3 : Outer join (left,right or both)

      kidsDF.join(teamsDF,joinCondition,"left_outer")

      kidsDF.join(teamsDF,joinCondition,"right_outer")

      kidsDF.join(teamsDF,joinCondition,"outer")

      // Join Type 4 : Semi Joins



      // Join Type 5 : Anti Joins











        spark.stop()


    }
}
