// Databricks notebook source
import com.databricks.spark.sql.perf.tpcds.TPCDS
 
// Note: Declare "sqlContext" for Spark 2.x version
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
val tpcds = new TPCDS (sqlContext = sqlContext)
// Set:
val databaseName = "sqlendpoint5000" // name of database with TPCDS data.
sql(s"use $databaseName")

val resultLocation = "/mnt/azureblobnew5000/TPC-DS/tpcds_results" // place to write results
// val resultLocation = "wasbs://dbstorage1000d@scbpocdev.blob.core.windows.net/rd2_1_tb_adb_spark_tpcds_results" // place to write results
val iterations = 3 // how many iterations of queries to run.
val queries = tpcds.tpcds2_4Queries // queries to run.
val timeout = 24*60*60 // timeout, in seconds.
// Run:
val experiment = tpcds.runExperiment(
  queries, 
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)
experiment.waitForFinish(timeout)
