import org.apache.spark.sql.{SparkSession, SparkTransformer}


object QueryAnalyzer extends  App {

  val session = SparkSession.builder().appName("QueryAnalytics").master("local[*]").getOrCreate()

  import session.implicits._
  import session.sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val df = session.read
  .option("header", "true").
    option("inferSchema", "true")
    .csv(getClass.getResource("Baby_Names__Beginning_2007.csv").getPath)


  df.createOrReplaceTempView("maha")

  println(df.queryExecution.logical.maxRows)

  println(session.time(session.sql("select * from maha where county='KINGS'").queryExecution.logical.maxRows))
  println(session.time(session.sql("select * from maha where county='KINGS' ").rdd.countApprox(1000,0.9)))
  println(session.time(session.sql("select * from maha where county='KINGS'").count))
  println(session.time(session.sql("select * from maha where county='KINGS'").explain))


  println(session.time(session.sql("select * from maha where county='KINGS'").queryExecution.toStringWithStats))
  println(session.time(session.sql("select * from maha a,maha b where a.county=b.county and a.county='KINGS'").queryExecution.logical.toJSON))

/*  val cnt = session.time(df.count().toDouble)
/*  println("Count "+cnt)
  println("Approx "+session.time(df.rdd.countApprox(1000,0.8)))*/

  val colNames = df.columns
  val cols = colNames.map(a => expr("approx_count_distinct("+a+") as "+a))
  df.select(cols:_*).show
  val data = df.select(cols:_*).take(1).map(r =>{
      colNames.map(name => (name->r.getAs[Long](name)))
  }).flatMap(x=>x)

  val mapData = data.toMap


  mapData.mapValues(v => cnt/v.toDouble).foreach(println)

  SparkTransformer(session,"select substring(a,1,2) ,b from tbl  where x=1")*/


  Thread.sleep(1000000)

}
