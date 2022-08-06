package com.SparkLowConstruct

import org.apache.spark.sql.SparkSession

object Example10_SparkRDD_BroadcastVariables {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Broadcast variables in rdd")
      .master("local[2]")
      .getOrCreate()

    val state = Map(("NY", "New York"), ("CA", "Califirnia"), ("FL", "Florida"))
    val countries = Map(("USA", "United state of America"), ("In", "India"))

    val broadcastStates = spark.sparkContext.broadcast(state)
    val broadcastCountries = spark.sparkContext.broadcast(countries)

    val data = Seq(("James", "Smith", "USA", "CA"),
      ("Michael", "Rose", "USA", "NY"),
      ("Robert", "Williams", "USA", "CA"),
      ("Maria", "Jones", "USA", "FL"))

    val rdd = spark.sparkContext.parallelize(data)

    val rdd2 = rdd.map(f => {
      val country = f._3
      val state = f._4
      val fullCountry = broadcastCountries.value.get(country).get
      val fullstate = broadcastStates.value.get(state).get
      (f._1, f._2, fullCountry, fullstate)
    })

    println(rdd2.collect().mkString("\n"))


  }

}
