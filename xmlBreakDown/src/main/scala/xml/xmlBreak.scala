package xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.{SparkConf, SparkContext}

object xmlBreak {
  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("project1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    //Reading the xml file, make sure the xml is located in the path
    val url_df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag","rss")
      .load("G:/dataset/en.xml")

    url_df.printSchema()

    val channelDF = url_df.select("channel.item")
    /*Item attribute is broken down to get access to its internal attributes.
    Top Stories is present within Item (array). In order to present Top Stories,
    Item is broken to Struct for better accessibility
     */
    val breakdownDF = channelDF.withColumn("item",explode(col("item")))


    val titleDF = breakdownDF.select("item.title")
    titleDF.show(4,truncate = false)
    titleDF.printSchema()

    /*Top Stories is broken into 2 columns. Select API will allow to print only required columns.
    Change the split condition to change the column attributes.
     */
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
