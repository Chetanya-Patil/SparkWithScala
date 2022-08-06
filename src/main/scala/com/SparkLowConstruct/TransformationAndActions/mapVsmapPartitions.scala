package com.SparkLowConstruct.TransformationAndActions

object mapVsmapPartitions {

  def main(args: Array[String]): Unit = {

    /*
    -- map vs mapPartitions

    This rdd has 10000 rows and 10 partitions

    each partition holds 1000 records

    rdd.map(func)   ----- 10000 times (It will invoke funct 10000 times)

    rdd.mapPartitions(func)  ---- 10 times (As per number of partitions)


     */



  }

}
