package com.shivamolabanti.common

import java.io.InputStreamReader

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shiva on 3/27/17.
  */
object ReadConfig {
  def main(args: Array[String]) {

    val sparksession = SparkSession.builder().appName("Read_Config").enableHiveSupport().getOrCreate()
    val config = ReadConfig.read(sparksession.sparkContext, args(0))
    val hiveDB = config.getString("hive.hive-db")

    System.out.println("HIVE-DB :: " + hiveDB)
  }

  def read(sc: SparkContext, hdfsPath: String): Config = {
    val hadoopConfig: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConfig)
    val file: FSDataInputStream = fs.open(new Path(hdfsPath))

    val reader = new InputStreamReader(file)
    val config = try {
      ConfigFactory.parseReader(reader)
    } finally {
      reader.close()
    }

    return config
  }

}
