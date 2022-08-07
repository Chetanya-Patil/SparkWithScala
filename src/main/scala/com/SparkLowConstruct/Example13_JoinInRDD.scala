package com.SparkLowConstruct

import org.apache.spark.SparkContext

object Example13_JoinInRDD {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[2]","JoinInRDD")

  //import student data into a RDD
    val student = sc.textFile("C:\\Users\\chaitanya_patil\\IdeaProjects\\SparkWithScala\\src\\main\\scala\\com\\Student.txt")

  //import class data into a RDD
  val course = sc.textFile("C:\\Users\\chaitanya_patil\\IdeaProjects\\SparkWithScala\\src\\main\\scala\\com\\Course.txt")

  //import StudentClass data into a RDD
  val studentcourse = sc.textFile("C:\\Users\\chaitanya_patil\\IdeaProjects\\SparkWithScala\\src\\main\\scala\\com\\StudentCourse.txt")

  // tokenize the RDD, convert studentID and course cost to integer, notice it is array starting from 0

  val student1 = student.map(rec => (rec.split("\t")(0).toInt,rec.split("\t")(1)))
  val course1 = course.map(rec => (rec.split("\t")(0),rec.split("\t")(1),rec.split("\t")(2).toInt))
  val studentcourse1 = studentcourse.map(rec => (rec.split("\t")(0).toInt,rec.split("\t")(1)))

  //show data in the RDD
  student1.collect().foreach(println)
  course1.collect().foreach(println)
  studentcourse1.collect().foreach(println)

    /*we want to know each person how much they spend on the course
  select s.student, sum(c.cost)
  from student s
  inner join studentcourse sc on s.StudentID =sc.StudentID
  inner join course c  on sc.CourseID =c.CourseID
  group by s.student  */

  //student RDD join studentcourse RDD
    val join1 = student1.join(studentcourse1)
    join1.collect().foreach(println)

    //in order to join with course RDD, we need to remap the key value pair, join key should be courseid, we also need student name

    val join1Remap = join1.map (rec =>(rec._2._2, rec._2._1))
    join1Remap.collect().foreach(println)


    //only need course RDD  CourseID and cost field
    val course1b = course1.map (rec=>(rec._1, rec._3))
    course1b.collect().foreach(println)


    //join with course RDD
    val join2 = join1Remap.join(course1b)
    join2.collect().foreach(println)


    //only need name and cost to aggregation
    val join2b = join2.map (rec=>(rec._2._1, rec._2._2))
    join2b.collect().foreach(println)

    //aggregtion
    val result = join2b.reduceByKey ((acc, value) => acc+value)
    result.collect().foreach(println)












































  }

}
