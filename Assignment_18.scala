import org.apache.spark.sql.SparkSession

object Assignment_18 {

  def main(args: Array[String]): Unit = {

        //Create spark object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Basic Example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Craete RDD for the given collection by using the spark context parallelize method

    val objList = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    //Task 1.1 Find the sum Of all the numbers
    val sumOfAll = objList.sum()
    println("Sum Of all the Numbers from the List: "+ sumOfAll)

    val sumUsingReduce = objList.reduce(_+_)
    println("Sum Of all the Numbers from the List (Reduce): "+ sumUsingReduce)

    //Task 1.2 find the total elements in the list
    val totalElementCount = objList.count()
    println("Total element count from the List: "+ totalElementCount)

    //1.3	calculate the average of the numbers in the list
    val average = sumOfAll / totalElementCount
    println("The average of the numbers in the list: "+ average)

    //Task 1.4 find the sum of all the even numbers in the list
    //Iterate through the list and write lambda expression to get even numbers and then add those
    val sumOfEven = objList.filter(f=>f%2==0).sum()
    println("Sum Of all the Even Numbers from the List: "+ sumOfEven)

    //Task 1.5 find the total number of elements in the list divisible by both 5 and 3
    //Iterate through the list and write lambda expression to get a number which is divisible by 5 and 3
    val sumOFdivisubleBy5Or3 = objList.filter(f=>(f%3==0) &&( f%5==0)).sum()
    println("Sum Of all the Numbers divisible by 5 Or 3 from the List: "+ sumOFdivisubleBy5Or3)
  }

}
