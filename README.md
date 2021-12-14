---

# Data Engineer Technical Challenge
***
![Scala](https://img.shields.io/badge/scala-%23DC322F.svg?style=for-the-badge&logo=scala&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4298B8.svg?style=for-the-badge&logo=Apache+Spark&logoColor=white)
![IntelliJ IDEA](https://img.shields.io/badge/IntelliJIDEA-000000.svg?style=for-the-badge&logo=intellij-idea&logoColor=white)
![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)

![Java](https://img.shields.io/badge/java-%23ED8B00.svg?style=for-the-badge&logo=java&logoColor=white)
![Markdown](https://img.shields.io/badge/markdown-%23000000.svg?style=for-the-badge&logo=markdown&logoColor=white)

> **`Prerequisites`**
>>Make sure the system is installed with **Scala** `2.13.0`, **Apache Spark** `3.0.3` and **IntelliJ IDE.**\
>> Make sure the source directory path is specified. By default, XML data is present in ==G:/dataset/en.xml"==.\
>> The file is read as ==en.xml== and winutils from https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe.

## Environment Setup
+ Ensure Java `version = 1.8` is setup in both user variables and system variables.
+ Hadoop support file ==winutils.exe== is also added to the path as HADOOP_HOME.
+ SPARK_HOME and HADOOP_HOME are added to the path.






## Project Environment Setup

+ Initially create a Scala **Maven Project** in _IntelliJ_.
+ Create **Package**, 
  - Inside package create **Scala Object**
+ Add _**Scala compiler**_ and necessary _**Spark Jars**_ in dependencies under **Project Structure** in **Files**.
+ The Scala project structure is as follows,
  + projectName **eg: xmlBreakDown**
    +  src
       +  main
           +  scala
               +  packageName **eg: xml**
                     +  fileName.scala **eg: xmlBreak.scala**
 

## Code
This section demonstrates the flow of code,
+ Lets begin the code by creating `Scala main()` and `Spark Session`.
+ Making use of Spark APIs will recommend to import relevant APIs if the Spark Jars are properly linked.


 Most of the work is accomplished with `SparkSession`. While `SparkContext` helps to modify _**hadoop configurations**_ which is responsible for writing data to _Windows file system_.


**Spark does not support XML formats implicitly. Parsing XML formats in Spark is possible with `Spark-XML.jar`, which is the reason for `format("com.databricks.spark.xml")`.**

Reading XML file in Spark

    val url_df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag","rss")
      .load("G:/dataset/en.xml")

From the above code chunk, load() holds the absolute location of the xml data.



## Substitution approach for Code Execution

+ If working along the code with the setup seems terrible, then I have simple way to workaround and the simple method is to run the entire code in `spark-shell` by executing the complete code from **xmlBreak.scala**. However this is not recommended as certain operations are hardly achieved.
+ As XML is involved, spark-shell should be explicitly provided with `spark-xml.jar`.
+ Open command prompt
```
spark-shell --jars pathofJarlocationWithCommaSeperation
```
The result shows the break down of Top Stories with 2 columns
+ Department
+ Feeds
```
+-----------------------------------------+---------------------------------------------------+
|Department                               |Feed                                               |
+-----------------------------------------+---------------------------------------------------+
|Transparent taxation                     |The fight for a fair and transparent tax system    |
|Culture                                  |How the EU supports culture                        |
|European Parliament’s Sakharov Prize 2021|Alexei Navalny                                     |
|Food and agriculture                     |Ensuring a safe, sustainable and secure food supply|
|State of the European Union Debate 2021  |Addressing Europeans' concerns                     |
|Digital transformation in the EU         |Benefitting people, the economy and the environment|
|LUX Audience Award 2021                  |The best of European cinema                        |
|The future is in your hands              |Conference on the Future of Europe                 |
|International Women’s Day 2021           |Women leading the fight against Covid-19           |
|Vaccines against Covid-19                |Ensuring safe vaccines for EU citizens             |
+-----------------------------------------+---------------------------------------------------+
```
The final result give 
intricate insights to the feeds.
[code_LINK](https://github.com/nishanthMatta/DE_Technical_Challenge/blob/main/xmlBreakDown/src/main/scala/xml/xmlBreak.scala)

## Final Insights
+ The code solves the basic requirement (_**retrieve the Top Stories**_). The code hardly requires changes to fetch similar records.
+ The delimiter "`-`" used to distinguish the column values.
+ TO achieve portability, code needs to satisfy the prerequisites.
+ Anyone who is interested to work around can follow this document and making changes to the relevant section of code.
