package xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._


object xmlBreak {
  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("project1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    val url_df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag","rss")
      .load("G:/dataset/en.xml")

    url_df.printSchema()

    val channelDF = url_df.select("channel.item")

    val breakdownDF = channelDF.withColumn("item",explode(col("item")))

    breakdownDF.show()
    breakdownDF.printSchema()
    //val sepdf = explodeDF.flatMap(x=>x.(","))

    val titleDF = breakdownDF.select("item.title")
    titleDF.show(4,truncate = false)
    titleDF.printSchema()

    val splitCol = titleDF.select(
      split(col("title")," - ").getItem(1).as("Department"),
      split(col("title")," - ").getItem(2).as("Feed"))
   splitCol.printSchema()
    println("Top Feeds")
    splitCol.show(truncate = false)

    splitCol.coalesce(1)
      .write.mode("overwrite")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
      .option("csv.enable.summary-metadata", "false")
      .option("header",value = true)
      .csv("C:/processed_feeds/")

    println("Data Successfully saved to the path")
  }
}
