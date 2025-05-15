// Databricks notebook source
val catalog = "db005"
val schema = "default"
val volume = "volume"
val downloadUrl = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
val fileName = "baby_names.csv"
val tableName = "baby_names"
val pathVolume = s"/Volumes/${catalog}/${schema}/${volume}"
val pathTable = s"${catalog}.${schema}"
print(pathVolume) // Show the complete path
print(pathTable) // Show the complete path

// COMMAND ----------

dbutils.fs.cp(downloadUrl, s"${pathVolume}/${fileName}")

// COMMAND ----------

val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", ",")
  .csv(s"${pathVolume}/${fileName}")

// COMMAND ----------

display(df)

// COMMAND ----------

val dfRenamedColumn = df.withColumnRenamed("First Name", "First_Name")
// when modifying a DataFrame in Scala, you must assign it to a new variable
dfRenamedColumn.printSchema()

// COMMAND ----------

dfRenamedColumn.write.mode("overwrite").saveAsTable(s"${pathTable}.${tableName}")