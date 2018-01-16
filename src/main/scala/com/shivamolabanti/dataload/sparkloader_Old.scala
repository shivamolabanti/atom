package com.shivamolabanti.dataload

import com.shivamolabanti.common.ReadConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object sparkloader_Old {
  def main(args: Array[String]) {

    // Default Values
    var SQLdbtable = "KPI_FACT_AR_TRANSACTIONS"

    if (args.length > 0)
      SQLdbtable = args(0)
    else {
      println("Target Table information is missing")
    }

    var SQLdbtable_Query = s"$SQLdbtable"

    // Create a SparkContext to initialize Spark

    // Cluster Mode:
    val conf = new SparkConf().setAppName(s"$SQLdbtable").set("spark.rdd.compress","true").set("spark.sql.caseSensitive","false").set("spark.shuffle.service.enabled","true")
    val sc = SparkContext.getOrCreate(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val hiveContext = new HiveContext(sc)


    // Read application.conf file
    val hdfsPath = "/deploy/conf/application.conf"
    val config = ReadConfig.read(sc, hdfsPath)

    //Get the Application properties from application.conf file
    val hiveDB = config.getString("hive.hive-db")
    val TargetTable = config.getString(s"datamodel_info.$SQLdbtable.hive-table")
    val datamodelpath = config.getString(s"app.datamodel-path")
    val datamodelfile = config.getString(s"datamodel_info.$SQLdbtable.datamodel-filename").toUpperCase


    // construct the file path from the config file
    val datamodelscript = scala.io.Source.fromFile(datamodelpath+datamodelfile).mkString

     // Set the Target Database to be used to load the data
    hiveContext.sql(s"USE $hiveDB")


    //HIVE Dynamic Partition settings
    /*hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hiveContext.setConf("hive.exec.max.dynamic.partitions", "100000")
    hiveContext.setConf("hive.exec.max.dynamic.partitions.pernode", "100000")
    hiveContext.setConf("hive.tez.container.size", "6144")
    hiveContext.setConf("hive.tez.java.opts", "-Xmx4096m")*/
    sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
    hiveContext.setConf("spark.sql.orc.filterPushdown", "true")
    sqlContext.sql("set spark.sql.caseSensitive=false")

    // Create Spark Dataframe with datamodel script
    val target_DF = sqlContext.sql(datamodelscript)

    val sql_df = sqlContext.sql("SELECT 'INV' TRX_SOURCE,\n  T.CUSTOMER_TRX_ID RECORD_ID,\n  D.CUST_TRX_LINE_GL_DIST_ID DISTRIBUTION_ID,\n  T.CUSTOMER_TRX_ID ,\n  0 CASH_RECEIPT_ID,\n  0 ADJUSTMENT_ID,\n  CASE\n    WHEN AP.CLASS = 'CM'\n    THEN T.CUSTOMER_TRX_ID\n    ELSE 0\n  END CREDIT_MEMO_ID,\n  T.BILL_TO_CUSTOMER_ID,\n  T.BILL_TO_SITE_USE_ID,\n  T.BILL_TO_CONTACT_ID,\n  T.SHIP_TO_CUSTOMER_ID,\n  T.SHIP_TO_SITE_USE_ID,\n  T.SHIP_TO_CONTACT_ID ,\n  T.PRIMARY_SALESREP_ID,\n  HCA.PARTY_ID,\n  NVL(SHIP_HCA.PARTY_ID,0) SHIP_PARTY_ID,\n  AP.CLASS TRX_TYPE,\n  AP.STATUS TRX_STATUS,\n  T.ORG_ID,\n  T.SET_OF_BOOKS_ID LEDGER_ID,\n  T.TRX_NUMBER TRX_NUMBER,\n  T.TRX_DATE TRX_DATE,\n  T.INVOICE_CURRENCY_CODE TRX_CURRENCY,\n  LD1.CURRENCY_CODE LEDGER_CURRENCY,\n  D.CODE_COMBINATION_ID,\n  D.AMOUNT TRX_CURRENCY_AMOUNT,\n  D.ACCTD_AMOUNT LEDGER_CURRENCY_AMOUNT,\n  0 ACCTD_AMOUNT_APPLIED_FROM,\n  0 ACCTD_AMOUNT_APPLIED_TO,\n  D.GL_DATE,\n  T.CREATION_DATE TRX_CREATION_DATE,\n  T.LAST_UPDATE_DATE TRX_LAST_UPDATE_DATE\nFROM \nRA_CUSTOMER_TRX_ALL T INNER JOIN RA_CUST_TRX_LINE_GL_DIST_ALL D ON (T.CUSTOMER_TRX_ID = D.CUSTOMER_TRX_ID)\nLEFT OUTER JOIN AR_PAYMENT_SCHEDULES_ALL AP ON (T.CUSTOMER_TRX_ID= AP.CUSTOMER_TRX_ID)\nINNER JOIN GL_LEDGERS LD1 ON (T.SET_OF_BOOKS_ID     = LD1.LEDGER_ID)\nINNER JOIN HZ_CUST_ACCOUNTS HCA ON (T.BILL_TO_CUSTOMER_ID = HCA.CUST_ACCOUNT_ID)\nLEFT OUTER JOIN HZ_CUST_ACCOUNTS SHIP_HCA ON (T.SHIP_TO_CUSTOMER_ID = SHIP_HCA.CUST_ACCOUNT_ID)\nWHERE D.ACCOUNT_CLASS    = 'REC'\nAND D.LATEST_REC_FLAG  = 'Y'\nUNION ALL\nSELECT 'CM_APP' TRX_SOURCE,\n  R.RECEIVABLE_APPLICATION_ID RECORD_ID,\n  0 DISTRIBUTION_ID,\n  R.APPLIED_CUSTOMER_TRX_ID CUSTOMER_TRX_ID,\n  0 CASH_RECEIPT_ID,\n  0 ADJUSTMENT_ID,\n  R.CUSTOMER_TRX_ID CREDIT_MEMO_ID,\n  T.BILL_TO_CUSTOMER_ID,\n  T.BILL_TO_SITE_USE_ID,\n  T.BILL_TO_CONTACT_ID,\n  T.SHIP_TO_CUSTOMER_ID,\n  T.SHIP_TO_SITE_USE_ID,\n  T.SHIP_TO_CONTACT_ID ,\n  T.PRIMARY_SALESREP_ID,\n  HCA4.PARTY_ID,\n  NVL(SHIP_HCA.PARTY_ID,0) SHIP_PARTY_ID,\n  R.APPLICATION_TYPE TRX_TYPE,\n  R.STATUS TRX_STATUS,\n  R.ORG_ID,\n  R.SET_OF_BOOKS_ID LEDGER_ID,\n  NULL TRX_NUMBER,\n  R.APPLY_DATE TRX_DATE,\n  T.INVOICE_CURRENCY_CODE TRX_CURRENCY,\n  LD4.CURRENCY_CODE LEDGER_CURRENCY,\n  R.CODE_COMBINATION_ID,\n  R.AMOUNT_APPLIED TRX_CURRENCY_AMOUNT,\n  ROUND(R.AMOUNT_APPLIED*NVL(T.EXCHANGE_RATE,1), 8) LEDGER_CURRENCY_AMOUNT,\n  R.ACCTD_AMOUNT_APPLIED_FROM,\n  R.ACCTD_AMOUNT_APPLIED_TO,\n  R.GL_DATE,\n  R.CREATION_DATE TRX_CREATION_DATE,\n  R.LAST_UPDATE_DATE TRX_LAST_UPDATE_DATE\nFROM\nAR_RECEIVABLE_APPLICATIONS_ALL R LEFT OUTER JOIN RA_CUSTOMER_TRX_ALL T ON (R.APPLIED_CUSTOMER_TRX_ID = T.CUSTOMER_TRX_ID)\nINNER JOIN GL_LEDGERS LD4 ON (R.SET_OF_BOOKS_ID = LD4.LEDGER_ID)\nLEFT OUTER JOIN HZ_CUST_ACCOUNTS HCA4 ON (T.BILL_TO_CUSTOMER_ID = HCA4.CUST_ACCOUNT_ID)\nLEFT OUTER JOIN HZ_CUST_ACCOUNTS SHIP_HCA ON (T.SHIP_TO_CUSTOMER_ID = SHIP_HCA.CUST_ACCOUNT_ID)\nWHERE R.APPLICATION_TYPE = 'CM'")

    // Write data into a Target table
   target_DF.write.format("orc").mode(SaveMode.Overwrite).saveAsTable(s"$TargetTable")

    // Stop the SC Context to free up the resources
    sc.stop()


  }

}
