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




