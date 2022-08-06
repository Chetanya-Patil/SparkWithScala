package com.SparkLowConstruct

import org.apache.spark.sql.SparkSession

object Example11_SparkDataframeBroadcastVariables {



  def main(args: Array[String]):Unit = {

  val spark = SparkSession.builder()
    .appName("Broadcast variable in dataframe")
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

    val columns = Seq("firstname","lastname","country","state")
    import spark.sqlContext.implicits._

    val df = data.toDF(columns:_*)

    df.printSchema()
    df.show()

    val df2 = df.map(row => {
      val country = row.getString(2)
      val state = row.getString(3)

      val fullCountry = broadcastCountries.value.get(country)
      val fullstate = broadcastStates.value.get(state)
      (row.getString(0),row.getString(1),fullCountry,fullstate)
    }).toDF(columns:_*)

    df2.show(false)

  }

}
