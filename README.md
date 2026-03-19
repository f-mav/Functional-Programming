# Functional-Programming

# Application Scenario 1: Reuters News Stories Analysis using Spark RDDs
Code and Run Instructions: The implementation of this application scenario will be found in the project : Reuters, with main class Main.scala.The created jar is : ReutersAnalysis.jar.
Firstly, we execute it locally on spark: spark-submit --master local[*] location/ ReutersAnalysis.jar
After that we comment the local configurations and uncommend the configuration for the cluster on our code. We have uploaded our jar file on the clusterr and we ran it there.
By observing our code we can see that there are used transformations such as map, join, reduceByKey, which due to Spark’s lasy evaluation do not create a job (they only build up a DAG that explain their execution. The description of our job is” runJob at SparkHadoopWriter.scala” Therefore, as explained above, the only transformation that creates a job is the “catNameStemIdJac.saveAsTextFile(outputFileDestination)” at Main:139. That’s the reason we have one job. 
We can observe the DAG for this job:
<img width="940" height="371" alt="εικόνα" src="https://github.com/user-attachments/assets/5a9c5dbc-6fed-428b-b703-400858b94f71" />

We can observe that indeed the stages for our job are split according to the action of our application. Some examples: 
Stage 0: textFile (Main:67) and map (Main:68)
```
// termId, stem
val termIdAndTermName = spark.sparkContext.textFile(stemTermMapInput)
  .map(line => {
    val words = line.split("\\s+")
    (words(1), words(0))
})
```

Additional Observations
We used to have the distinct transformation applied, but we decided to remove it (it is still commented in the code), because it didn’t change the result and it made our code slower.
There are provided some screenshots from our application before the remove of distinct (locally not on the cluster). Also in this is before we add the join (so that we print the category name instead of the id). 





Stage 1: textFile and flatmap (lines 56 to 64)
```
val docIdAndTermId = spark.sparkContext.textFile(docTermsInput)
  .flatMap(line => {
    *codeblock*})
```

Stage 2: join(Main:75) , map (Main:76), map(Main:94), map (Main:98)
```
val docIdAndTermName = docIdAndTermId
  .join(termIdAndTermName)
  .map({
    case (termId, (docId, termName)) => (docId, termName)
  })
[…]
val counts2 = docIdAndTermName
  .map ({
    case (docId, termName) => (termName, docId)
  })
    .map ({ case (termName, docId) => (termName, 1)
  })
```

At this part we can see that the following  line is:
```
.reduceByKey(_ + _)//count for each key (termName) how many times it occurs
```
Which belongs to stage 9. This happens because reduceByKey is a wide transformation, therefore data from different partitions need to be combined (shuffling and marking the start of a new stage).
Stage 3: join(Main:75) , map (Main:76)
```
val docIdAndTermName = docIdAndTermId
  .join(termIdAndTermName)
  .map({
    case (termId, (docId, termName)) => (docId, termName)
  })
```


# Application Scenario 2: Vessel Trajectory Analytics in the Aegean Sea using Spark Dataframes
Locally, we can run our application : spark-submit --master local[*]   ...\AegeanAnalysis\out\artifacts\AegeanAnalysis_jar\AegeanAnalysis.jar
On the cluster:
cd /usr/hdp/current/spark2-client
./bin/spark-submit --class Main \
--master yarn --deploy-mode cluster \
--driver-memory 4g \
--num-executors 3 --executor-memory 2g --executor-cores 1 \
/home/fp7/AegeanAnalysis.jar

Observations : The main difference is that now we have 19 jobs instead of 1. This happens because we have more actions that requires the computation of the transformations applied our DataFrame. For example, the show() action also creates a new job and we save 5 different outputs.
For example this is the analysis with the respective code for  job 8: 
<img width="660" height="414" alt="εικόνα" src="https://github.com/user-attachments/assets/cd358dbc-b75b-43f8-9134-89f421fefb9b" />

This stage shows along the other transformations the aggregation (agg()) that happens , we show it to see how a different transformation is shown. We observe that due to the fact that it is a wide transformation we change stage (as shown before with reduceByKey). 
The code for job 8 is :
```
val q3 = vesselsInBothSameDay
  .join(aegeanDfDayDf, Seq("mmsi", "timestamp"))//join the found vessels with the df holding mmsi and timestamp
  .agg(avg("speedoverground").alias("avg_sog"))//3rd column will be the avg sppedoverground

q3.show()
```
# Citation
Eclass : INF424 subject material  (this project was executed for the completion of this class)
https://subhamkharwal.medium.com/pyspark-the-factor-of-cores-e884b2d5af6c
https://dzone.com/articles/apache-spark-performance-tuning-degree-of-parallel
https://sparkbyexamples.com/spark/what-is-spark-stage/
https://sparkbyexamples.com/spark/what-is-spark-job/



                                                                                                                             




