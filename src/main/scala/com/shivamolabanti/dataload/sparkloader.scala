package com.shivamolabanti.dataload

import com.shivamolabanti.common.ReadConfig
import org.apache.spark.sql.{SaveMode, SparkSession}


object sparkloader {
  def main(args: Array[String]) {

    // Default Values
    var SQLdbtable = "KPI_FACT_AR_TRANSACTIONS"

    if (args.length > 0)
      SQLdbtable = args(0)
    else {
      println("Target Table information is missing")
    }

    var SQLdbtable_Query = s"$SQLdbtable"

    //Create Spark session and enable HIVE Support
    val sparksession = SparkSession.builder().appName(s"$SQLdbtable").config("spark.rdd.compress","true").config("spark.sql.caseSensitive","false").config("spark.shuffle.service.enabled","true").enableHiveSupport().getOrCreate()

    // Read application.conf file
    val hdfsPath = "/deploy/conf/application.conf"
    val config = ReadConfig.read(sparksession.sparkContext, hdfsPath)

    //Get the Application properties from application.conf file
    val hiveDB = config.getString("hive.hive-db")
    val TargetTable = config.getString(s"datamodel_info.$SQLdbtable.hive-table")
    val datamodelpath = config.getString(s"app.datamodel-path")
    val datamodelfile = config.getString(s"datamodel_info.$SQLdbtable.datamodel-filename").toUpperCase


    // construct the file path from the config file
    val datamodelscript = scala.io.Source.fromFile(datamodelpath+datamodelfile).mkString

    import sparksession.sql


    // Set the Target Database to be used to load the data
    sql(s"USE $hiveDB")

    // Few HIVE performance settings
    sparksession.conf.set("spark.sql.orc.filterPushdown", "true")
    sparksession.conf.set("spark.sql.orc.filterPushdown", "true")
    sql("set spark.sql.caseSensitive=false")

    // Create Spark Dataframe with datamodel script
    val target_DF = sql(datamodelscript)

    // Write data into a Target table
    target_DF.write.format("orc").mode(SaveMode.Overwrite).saveAsTable(s"$TargetTable")

    // Stop the Spark Session to free up the resources
    sparksession.stop()

  }

}
