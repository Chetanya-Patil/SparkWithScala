package com.SparkLOWConstruct.CacheVsPersist

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

// Spark Cache and persist are optimization technique to improve the performance.

// Using cache() and persist() methods, Spark provides an optimization mechanism to store the intermediate computation of an RDD,
// Dataframe, and Dataset so they can be reused in subsequent actions( Reusing the RDD, Dataframe, and Dataset computation result's.

// Both caching and persisting are used to save the Spark RDD, Dataframe, and Datasets's. But, the difference is, RDD cache()
// method is used to store it to the uer-defined storage level.

// Advantages for Caching and Persistance

// 1. Cost efficient
// 2. Time efficient
// 3. Execution time

/*
-- Cache()
Spark DataFrame or Dataset caching by default saves it to storage level `MEMORY_AND_DISK` because recomputing the in-memory columnar representation of the underlying table is expensive.
Note that this is different from the default cache level of `RDD.cache()` which is ‘MEMORY_ONLY‘
 */

object Example1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Cache")
      .getOrCreate()

    import spark.implicits._
    val columns = Seq("Seqno","Quote")
    val data = Seq(("1","Be the change that you want to see in the world"),
      ("2","Everyone thinks of changing the world, but no one thinks of changing himself"),
      ("3","The purpose of our lives is to be happy"))

    val df = data.toDF(columns:_*)

    val dfCache = df.cache()
    dfCache.show()

    //-- Spark Persist syntax
    // Spark persist has two signature first signature doesn’t take any argument which by default saves it to MEMORY_AND_DISK storage level
    // and the second signature which takes StorageLevel as an argument to store it to different storage levels.

    val dfPersist = df.persist()
    dfPersist.show(false)

    val dfPersist2 = df.persist(StorageLevel.MEMORY_ONLY)
    dfPersist2.show(false)

    // --unpersist()

    val dfPersist3 = dfPersist.unpersist()
    dfPersist3.show(false)

    // Spark Persistance storage levels



   // println(spark.conf.get("spark.serializer"))


    scala.io.StdIn.readLine()



  }

}
