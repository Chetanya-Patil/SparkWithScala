package com.SparkWithScala.SparkSQL_Transformations
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

  // Add and Update Column
  /*
    1. Add a New Column to Dataframe
    2. Change value of an Existing Column
    3. Derive New Column From an Existing Column
    4. Change Column Data Type
    5. Add, Replace or Update Multiple Columns
    6. Rename Column Name
    7. Drop a column from Dataframe
    8. Split Column into Multiple Columns

    SYNTAX:
    withColumn(colName:String,col: Column):DataFrame
    colName:String ---- New column or old column name
    col:Column ---column expression
   */

object withColumn {
  def main(args:Array[String]):Unit = {

    val data = Seq(Row(Row("James;","","Smith"),"36636","M","3000"),
      Row(Row("Michael","Rose",""),"40288","M","4000"),
      Row(Row("Robert","","Williams"),"42114","M","4000"),
      Row(Row("Maria","Anne","Jones"),"39192","F","4000"),
      Row(Row("Jen","Mary","Brown"),"","F","-1")
    )

    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",StringType)

    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("withColumn Execution")
      .getOrCreate()

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)

    df.show()
    df.printSchema()

    // 1. Add New Column to DataFrame

    df.withColumn("Country", lit("USA")).show()

    // chaining to operate on multiple columns
    df.withColumn("Country",lit("USA"))
      .withColumn("anotherColumn",lit("anotherValue")).show()


    // Above approach is fine if you are manipulating few Columns, but when you wanted to add or update multiple columns,
    // do not use the chaining withColumn() as it leads to performance issues, Use select() to update multiple columns instead.

    // 2. Change Value of an Existing Column

    // first argument is string type
    // Second argument is Column type

    df.withColumn("salary",col("salary")*100).show()

    // The Snippet multiplies the value of "salary" with 100 and updates the value back to "salary" column.

    // 3. Derive New Column From and Existing Column

    df.withColumn("IncrementedSalary",col("salary")*10).show()

    // The snipprt creates a new column "IncrementedSalary" by multiplying "salary" column with value 10

    // 4. Add, Replace, or Update multiple Columns

        /*
          When you wanted to add, replace or update multiple columns in spark DataFrame, It is not suggestible to chain
          withColumn() function as it leads into performance issue and recommends to use select() after creating a temporary view on Dataframe.
         */
    val df2 = df.select("*")

    df2.createOrReplaceTempView("PERSON")
    spark.sql("SELECT salary*100 as salary,salary*-1 as CopiedColumn, 'USA' as country FROM PERSON").show()

    // 5. Rename Column Name

        df.withColumnRenamed("dob","dateOfBirth").show()

    // 6. Drop a Column

        df.drop("CopiedColumn")

    // 7. Change Column Data Type (Important) ---How to change Column Type ?

     //file_Path = src/main/scala/com/SparkWithScala/SparkSQL_CreateDataFrame/HowToChangeColumnType.scala

    // 8. Split Column into Multiple Columns (Important)

     // file_path = src/main/scala/com/SparkWithScala/SparkSQL_CreateDataFrame/SplitColumnIntoMultipleColumns.scala






















  }

}
