package com.SparkWithScala.SparkSQL_CreateDataFrame

import org.apache.spark.sql.SparkSession

object SelectColumns {

  def main(args: Array[String]): Unit = {


  val spark = SparkSession.builder
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq(("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  )
  val columns = Seq("firstname","lastname","country","state")
  import spark.implicits._
  val df = data.toDF(columns:_*)
  df.show(false)

  // 1. Select and Multiple Columns

  df.select("firstname","lastname").show()

  //using dataframe object name
  df.select(df("firstname"),df("lastname")).show()

  //using col function, use alias() to get alias name
  import org.apache.spark.sql.functions.col
  df.select(col("firstname").alias("fname"),col("lastname").alias("lname"))

  // 2. Select Multiple Column

    /*
    Here, we use df.columns to get all columns on a Dataframe as Array[String], convert it to Array[Column] using scala map()
    and finally use it on select()
     */

  df.select("*").show()

  val columnsAll = df.columns.map(x => col(x))

  df.select(columnsAll:_*).show()

  // 3. Select Columns from List

       /*
       Some times you may have to select column names frorm an Array, List or Seq of string, below example provides snippet
       how to do this using list.
        */

    val listCols = List("lastname","country")
    df.select(listCols.map(x => col(x)):_*).show()

    // 4. Select First N Columns

        /*
        In order to select first N columns,you can use the df.columns to get all the columns on Dataframe and use the slice() method
        to select the first n columns.Below snippet select first 3 columns.
         */

    //select first 3 columns
    df.select(df.columns.slice(0,3).map(x => col(x)):_*).show()


    //5. Select Columns by Position or index

    //Select 4th column ( index starts from zero)
    df.select(df.columns(3)).show()

    //Select columns from index 2 to 4
    df.select(df.columns.slice(2,4).map(x => col(x)):_*).show()


    //6. Select columns by regular expression

        /*
        You can use df.colRegex() to select columns based on a regular expression. The below example shows all columns that contains
        name string.
         */

      // select column b regular expression
      df.select(df.colRegex("`^.*name*`")).show()


    // 7. Select Columns Starts or Ends with

      df.select(df.columns.filter(x => x.startsWith("first")).map(y => col(y)):_*)

      df.select(df.columns.filter(x => x.endsWith("name")).map(y => col(y)):_*)


    // 8. Select Nested Struct Columns






















  }
}
