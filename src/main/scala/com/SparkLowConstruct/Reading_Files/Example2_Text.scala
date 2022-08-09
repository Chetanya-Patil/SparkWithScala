package com.SparkLowConstruct.Reading_Files

import org.apache.spark.SparkContext

object Example2_Text {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[2]","Reading Text Files")




  }

}
