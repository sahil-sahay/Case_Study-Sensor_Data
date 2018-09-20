package SQL
import org.apache.spark.sql.SparkSession

object CaseStudy {

  //Create a case class hvac_cls to be used globally inside the main method.
  //Inferring the Schema Using Reflection.Automatically converting an RDD containing case classes to a DataFrame.
  //The case class defines the schema of the table. The names of the arguments to the case class are read using reflection
  //and become the names of the columns.
  case class hvac_cls(Date:String,Time:String,TargetTemp:Int,ActualTemp:Int,System:Int,SystemAge:Int,BuildingId:Int)
  case class building(buildingid:Int,buildmgr:String,buildAge:Int,hvacproduct:String,Country:String)


  // Main method - The execution entry point for the program
  def main(args: Array[String]): Unit  = {
    //println("hey scala")

    //Let us create a spark session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL Use Case A21_Sensor ")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //Set the log level as warning
    spark.sparkContext.setLogLevel("WARN")


    //println("Spark Session Object created")

    // Reading a file for the hvac_cls schema created above
    val data = spark.sparkContext.textFile("C:\\Users\\ssahay\\Documents\\Acadgild\\Hadoop_Lectures\\Day_28_Final_Project\\HVAC.csv");

    //println("HVAC Data->>"+data.count())

    // Storing the first line that is header of the file into another variable
    val header = data.first()

    // Selecting all other lines into data1 excluding the header.
    val data1 = data.filter(row => row != header)

    //println("Header removed from the data !")

    //For implicit conversions like converting RDDs and sequences  to DataFrames
    import spark.implicits._

    //Converting data1 into dataFrame splitting on comma character (,)
    // trim is used to eliminate leading/trailing whitespace & converting values into respective data types
    val hvac = data1.map(x=>x.split(",")).map(x => hvac_cls(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toInt)).toDF()

    hvac.show()

//    println("HVAC Dataframe created !")

    //Objective 1 - Load HVAC.csv file into a temporary table.
    //Converting the above created schema into an SQL view named HVAC i.e. Loading file into temp table
    hvac.createOrReplaceTempView("HVAC")


    println("Dataframe Registered as table !")

    //Add a new column, tempchange - set to 1, if there is a change of greater than +/-5 between actual and target temperature
    //Added a new column based upon a condition
    val hvac1 = spark.sql("select *,IF((targettemp - actualtemp) > 5, '1', IF((targettemp - actualtemp) < -5, '1', 0)) AS tempchange from HVAC")

    hvac1.show()


    hvac1.createOrReplaceTempView("HVAC1")

    println("Data Frame Registered as HVAC1 table !")

    //Now lets load the second data set

    // Reading a file for the building schema created above
    val data2 = spark.sparkContext.textFile("C:\\Users\\ssahay\\Documents\\Acadgild\\Hadoop_Lectures\\Day_28_Final_Project\\building.csv");

    // Storing the first line that is header of the file into another variable
    val header1 = data2.first()

    // Selecting all other lines into data1 excluding the header.
    val data3 = data2.filter(row => row != header1)


//    println("Header removed from the building data")

//    println("Buildings Data->>"+data3.count())


    //Now let us create the building dataframe
    //Converting data3 into dataFrame splitting on comma character (,)
    //trim is used to eliminate leading/trailing whitespace & converting values into respective data types.
    val build = data3.map(x=> x.split(",")).map(x => building(x(0).toInt,x(1),x(2).toInt,x(3),x(4))).toDF

    build.show()

    //Converting the above created schema into an SQL view named building i.e. Loading building.csv as a temporary table
    build.createOrReplaceTempView("building")

    println("Buildings data registered as building table")

    //Now join the two tables building and hvac1 on buildingid
    val build1 = spark.sql("select h.*, b.country, b.hvacproduct from building b join hvac1 h on b.buildingid = h.buildingid")

    build1.show()


    //Selecting tempchange and country column from build1 DataFrame created above
    val tempCountry = build1.map(x => (new Integer(x(7).toString),x(8).toString))

//    tempCountry.show()

    //Filter the values where tempchange is 1
    val tempCountryOnes = tempCountry.filter(x=> {if(x._1==1) true else false})

//    tempCountryOnes.show()

    //Creating an Schema with column names as "Count","Country"
    val colName = Seq("Count","Country")

    //Schema of tempCountryOnes is (_1,_2),converting it into ("Count","Country")	in temp_country_count by specfying
    //above two columns as column names.
    val temp_country_count = tempCountryOnes.toDF(colName:_*)

    //Count the number of occurance for each country from the above fltered Dataset &
    // displaying the final output by sorting the count in descending order
    val final_df = temp_country_count.groupBy("Country").count
    final_df.orderBy($"count".desc).show // or final_df.sort($"count".desc).show
    /*tempCountryOnes.createOrReplaceTempView("temp_count")
    val temp_tab = spark.sql("select _2 as Country,count(_2) as count from temp_count group by _2")
    temp_tab.show*/
  }
}