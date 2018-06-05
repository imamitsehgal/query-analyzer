import QueryAnalyzer.getClass
import org.apache.spark.sql.SparkSession

object TestApp {

  val spark = SparkSession.builder().appName("QueryAnalytics").master("local[*]").getOrCreate()

  import spark.implicits._
  import spark.sqlContext.implicits._
  import org.apache.spark.sql.functions._


  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.cbo.enabled","true")
  spark.conf.set("spark.sql.statistics.histogram.enabled","true")
//  spark.sql("analyze table events compute statistics for columns eventID,symbol")
  val queries =  spark.read.text("/home/asehgal/queryanalyzerdemo/queries.txt").map(r=>r.getAs[String](0)).collect
  queries.foreach{query =>
    val exec = spark.sql(query).queryExecution
    val stats = exec.optimizedPlan.stats(spark.sessionState.conf)
    println(exec.toStringWithStats)
    println("--------------------------------------------------------------")
    println(s"For $query\n Estimated Row count ${stats.rowCount}")
    println(s"Estimated Bytes size of data ${stats.sizeInBytes}")
    println(s"Estimated Bytes size of data ${stats.sizeInBytes}")

    println("--------------------------------------------------------------")
  }

/*
  queries.foreach{query =>
    val stats = spark.sql(query).count
    println("--------------------------------------------------------------")
    println(s"For $query\nReal Row count ${stats}")
    println("--------------------------------------------------------------")
  }
*/


  System.exit(0)
}
