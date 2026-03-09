import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

object MainAnalysis extends App{

  val spark = SparkSession.builder
    .master("local[*]") //local[*] means "use as many threads as the number of processors available to the Java virtual machine"
    .appName("Tweet Word Count RDDs")
    .getOrCreate()

  //input paths
  val categDocInput = "hdfs://localhost:9000/reuters/rcv1-v2.topics.qrels"
  val docTermsInput = "hdfs://localhost:9000/reuters/lyrl2004_vectors_train.dat,hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt0.dat,hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt1.dat,hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt2.dat,hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt3.dat"
  //val docTermsInput = "hdfs://localhost:9000/reuters/lyrl2004_vectors_train.dat" //in case we want to run only the train data
  val stemTermMapInput =  "hdfs://localhost:9000/reuters/stem.termid.idf.map.txt"

  //output path
  val outputPath = "/reuters/reutersOutput"
  val outputFileDestination = "hdfs://localhost:9000/reuters/reutersOutput"


  //get an RDD with the form (docId, categoryName) for each line
  val docIdAndCatName = spark.sparkContext.textFile(categDocInput)
    .map(line => {
      val words = line.split("\\s+") //split into words
      (words(1), words(0))//words(0) is the categoryName and words(1) is the docId
    })

  //get an RDD with the form (docId, termId) for each line
  val docIdAndTermId = spark.sparkContext.textFile(docTermsInput)
    .flatMap(line => {
      val words = line.split("\\s+")//split into words
      val docId = words(0)//docId is words(0)
      words.tail.map(term => {//each term of tail is like this: termId:weight, but we only need the termId
        val termId = term.split(":")(0)//we split each term on : and we take the first
        (docId, termId)
      })
  })

  // termId, stem
  val termIdAndTermName = spark.sparkContext.textFile(stemTermMapInput)
    .map(line => {
      val words = line.split("\\s+")
      (words(1), words(0))
  })

  // |DOC(C)|
  val counts1 = docIdAndCatName
    .map({//to count the docs per category categoryName needs to be the key
      case (docId, categoryName ) => (categoryName, docId)
    })
    //.distinct() //does not make a difference and makes everything slower
    .map({//next to each categoryName we need to have 1 instead of docId to count the occurences
      case (categoryName, docId ) => (categoryName, 1)
    })
    .reduceByKey(_ + _)//count for each key (categoryName) how many times it occurs

  // |DOC(T)|
  val counts2 = docIdAndTermId.distinct()
    .map ({//similarly to count the docs per term we need to have the termId as the key
      case (docId, termId) => (termId, docId)
    })
    //.distinct()
    .map ({//next to each termId we have the occurences so far (1)
      case (termId, docId) => (termId, 1)
    })
    .reduceByKey(_ + _)//count for each key (termId) how many times it occurs


  // |DOC(T) ∩ DOC(C)|
  val counts3 = docIdAndCatName.join(docIdAndTermId)
    .map ({//here the key needs to be (termId, catName)
      case (docId, (catName, termId)) => ((termId, catName), docId)
    })
    //.distinct() //does not make a difference and makes everything slower
    .map ({//for each we have the occurences (1 at the first time)
      case ((termId, catName), docId) => ((termId, catName), 1)
    })
    .reduceByKey(_ + _)//add the occurences of each termId, category name combination


  val catNameStemIdJac = counts3
    .map ({//we need the |DOC(C)| for each catName
      case ((termId, catName), joinCount) => (catName, (termId, joinCount))//catName becomes the key
    })
    .join(counts1)//we join with counts1 to get the |DOC(C)| for each catName
    .map ({//we need the |DOC(T)| for each termId
      case (catName, ((termId, joinCount), docCCount)) => (termId, (catName, joinCount, docCCount))//termId becomes the key
    })
    .join(counts2)//we join with counts2 to get the |DOC(T) for each termId
    .map({ case (termId, ((catName, joinCount, docCCount), docTCount)) => {
      val unionCount = docTCount + docCCount - joinCount//we use the expression given in the exercise
      val jacardIndex = joinCount.toDouble / unionCount.toDouble//without .toDouble everything is rounded to 0
      (catName, termId, jacardIndex)
    }
    })



  //catNameStemIdJac.saveAsTextFile("hdfs://localhost:9000/reuters/output.txt")
  val hdfsURI = "hdfs://localhost:9000"
  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfsURI)
  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  hdfs.delete(new Path(outputPath), true)
  catNameStemIdJac.saveAsTextFile(outputFileDestination)

  //ta texts 04 kai 03 exoyn yliko gia anafora
  //counts.foreach(println)
  spark.stop() // Stop the SparkSession. DO NOT FORGET!!!

}
