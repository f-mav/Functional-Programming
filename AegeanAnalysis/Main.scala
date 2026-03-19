import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{abs, avg, col, collect_set, count, date_format, dayofweek, desc, hour, month, size}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, LongType, DateType}

object Main extends App {
//  val spark = SparkSession.builder
//    .master("local[*]") //local[*] means "use as many threads as the number of processors available to the Java virtual machine"
//    .appName("Tweet Word Count RDDs")
//    .getOrCreate()

  //configuration for the SoftNet cluster
  val spark = SparkSession.builder
    .appName("AegeanAnalysis")
    .master("yarn")
    .config("spark.hadoop.fs.defaultFS", "hdfs://clu01.softnet.tuc.gr:8020")
    //.config("spark.yarn.jars", "hdfs://clu01.softnet.tuc.gr:8020/user/xenia/jars/*.jar")
    .config("spark.hadoop.yarn.resourcemanager.address", "http://clu01.softnet.tuc.gr:8189")
    .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
    //.enableHiveSupport()
    .getOrCreate()

  //softnet aegeanPath
  val aegeanPath = "hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/nmea_aegean/nmea_aegean.logs"

  //local aegeanPath
  //val aegeanPath = "hdfs://localhost:9000/aegean/nmea_aegean.logs"

  //softnet output paths
  val q1output = "hdfs://clu01.softnet.tuc.gr:8020/user/fp7/Aegean/question1"
  val q2output = "hdfs://clu01.softnet.tuc.gr:8020/user/fp7/Aegean/question2"
  val q3output = "hdfs://clu01.softnet.tuc.gr:8020/user/fp7/Aegean/question3"
  val q4output = "hdfs://clu01.softnet.tuc.gr:8020/user/fp7/Aegean/question4"
  val q5output = "hdfs://clu01.softnet.tuc.gr:8020/user/fp7/Aegean/question5"

  //local outputh paths
//  val q1output = "hdfs://localhost:9000/aegean/question1"
//  val q2output = "hdfs://localhost:9000/aegean/question2"
//  val q3output = "hdfs://localhost:9000/aegean/question3"
//  val q4output = "hdfs://localhost:9000/aegean/question4"
//  val q5output = "hdfs://localhost:9000/aegean/question5"

  //we need to adjust each field to make sure it is in the correct form and data type
  val editedSchema = StructType(Array(
    StructField("timestamp", DateType, true),//we need the date of each timestamp
    StructField("station", IntegerType, true),//station is always integer
    StructField("mmsi", LongType, true),
    StructField("longitude", DoubleType, true),
    StructField("latitude", DoubleType, true),
    StructField("speedoverground", DoubleType, true),
    StructField("courseoverground", DoubleType, true),
    StructField("heading", IntegerType, true),
    StructField("status", IntegerType, true)
  ))

  val aegeanDfDayDf = spark.read
    .option("header", "true")
    .schema(editedSchema)
    .csv(aegeanPath)




  aegeanDfDayDf.show(100)

  val q1 = aegeanDfDayDf
    .groupBy("timestamp", "station")//group entries with same day and station and count the same appearances for each
    .count()

  q1
    .write
    .mode(SaveMode.Overwrite) // Choose the appropriate mode (overwrite, append, etc.)
    .format("csv") // Choose the desired format (parquet, csv, json, etc.)
    .save(q1output)

  q1.show()

  val q2 = aegeanDfDayDf
    .groupBy("mmsi")//for each mmsi count how many timestamps there are / each timestamp = a tracked position
    .agg(count("timestamp").alias("positions_count"))
    .orderBy(desc("positions_count"))//order by desc to go from highest to smaller
    .limit(1)//limit 1 to get the highest

  q2
    .write
    .mode(SaveMode.Overwrite) // Choose the appropriate mode (overwrite, append, etc.)
    .format("csv") // Choose the desired format (parquet, csv, json, etc.)
    .save(q2output)

  q2.show()

  //keep entries with stations 8006 and 10003
  val specStations = aegeanDfDayDf
      .where(col("station").like("8006")
      .or(col("station").like("10003")))


  val  vesselsInBothSameDay = specStations
    .groupBy("mmsi", "timestamp")//for an (mmsi, timestamp) create a column that has all the different stations
    .agg(collect_set("station").alias("stations"))
    .filter(size(col("stations")) === 2)//if there are two stations then the vessel was in both 8006 and 10003 in the same day
    .select("mmsi", "timestamp")//keep mmsi and timestamp
  //vesselsInBothSameDay.show(1000)

  val q3 = vesselsInBothSameDay
    .join(aegeanDfDayDf, Seq("mmsi", "timestamp"))//join the found vessels with the df holding mmsi and timestamp
    .agg(avg("speedoverground").alias("avg_sog"))//3rd column will be the avg sppedoverground

  q3.show()

  q3
    .write
    .mode(SaveMode.Overwrite) // Choose the appropriate mode (overwrite, append, etc.)
    .format("csv") // Choose the desired format (parquet, csv, json, etc.)
    .save(q3output)

  q3.show()


  val q4 = aegeanDfDayDf
    .withColumn("abs_head_minus_cog", abs(col("heading") - col("courseoverground")))//calculate the abs heading - cog for each vessel
    .groupBy("station")//group by station and find the avg of the above to find the average heading vs COG per station
    .agg(avg("abs_head_minus_cog")).alias("average_abs_head_minus_cog")


  q4
    .write
    .mode(SaveMode.Overwrite) // Choose the appropriate mode (overwrite, append, etc.)
    .format("csv") // Choose the desired format (parquet, csv, json, etc.)
    .save(q4output)

  q4.show()


  val q5 = aegeanDfDayDf
    .groupBy("status")//group by status and count how many times each status occurs
    .agg(count("status").alias("status_count"))
    .orderBy(desc("status_count"))//order by desc to go from the highest to the lowest
    .limit(3)//get the top 3

  q5
    .write
    .mode(SaveMode.Overwrite) // Choose the appropriate mode (overwrite, append, etc.)
    .format("csv") // Choose the desired format (parquet, csv, json, etc.)
    .save(q5output)

  q5.show()


  //val collectedData = aegeanDfDayDf.collect()


  spark.stop() // Stop the SparkSession. DO NOT FORGET!!!
}