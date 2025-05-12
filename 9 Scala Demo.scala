// Databricks notebook source
//Declare an immuatable list
val nums = List(10,20,30)
println(nums)

//Create a sequence of tuples and convert to dataFrame
val data = Seq((1,"Alice","Female"),(2,"Bob","Male"))
val df = spark.createDataFrame(data).toDF("id","name","gender")
df.show()

// COMMAND ----------

val salesData = Seq(
  ("2024-01-01", "North", "Product A", 10, 200.0),
  ("2024-01-01", "South", "Product B", 5, 300.0),
  ("2024-01-02", "North", "Product A", 20, 400.0),
  ("2024-01-02", "South", "Product B", 10, 600.0),
  ("2024-01-03", "East",  "Product C", 15, 375.0)
)
val df = spark.createDataFrame(salesData).toDF("date", "region", "product", "quantity", "revenue")
df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Total revenue for each region

// COMMAND ----------

import org.apache.spark.sql.functions._
df.groupBy("region").agg(sum("revenue")).show()