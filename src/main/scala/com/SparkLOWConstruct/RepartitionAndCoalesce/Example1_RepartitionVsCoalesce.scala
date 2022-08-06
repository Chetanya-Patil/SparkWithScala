package com.SparkLOWConstruct.RepartitionAndCoalesce

import org.apache.spark.SparkContext

object Example1_RepartitionVsCoalesce {

  def main(args: Array[String]):Unit = {

    val sc = new SparkContext("local[2]","Repartitions and Coalesce")

    val dummydata = List(1,2,3,4,5,6,7,8,9,10)

    val dummyDataRdd = sc.parallelize(dummydata)

    println(dummyDataRdd.getNumPartitions)

    //Here we have increase the number of paritions from 2 to 10
    val data = dummyDataRdd.repartition(10)

    println(data.getNumPartitions)

    // Here we have decrease the number of paritions
    val data2 = data.coalesce(2)

    println(data2)


    // To increase the number of parition preferred repartition only
    // To decrease the number of partition preferred coalesce only.





    scala.io.StdIn.readLine()
  }

}
