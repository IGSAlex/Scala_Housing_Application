
import org.apache.spark.sql.SparkSession
import scala.math.random
import oracle.jdbc.driver.OracleDriver 

object Spark_ExportToParquet {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local")
      .getOrCreate()
      
      
      var tables = List(
          List("F0005","dclvjdeapp-d","1521","JDE900D","E_IQ_CHECK_JDE_PY","CRPCTL","poc2017pw"),
List("F0010","dclvjdeapp-d","1521","JDE900D","E_IQ_CHECK_JDE_PY","CRPDTA","poc2017pw"),
List("F0901","dclvjdeapp-d","1521","JDE900D","E_IQ_CHECK_JDE_PY","CRPDTA","poc2017pw"),
List("F0911","dclvjdeapp-d","1521","JDE900D","E_IQ_CHECK_JDE_PY","CRPDTA","poc2017pw"),
List("F550901B","dclvjdeapp-d","1521","JDE900D","E_IQ_CHECK_JDE_PY","CRPDTA","poc2017pw"),
List("F550901C","dclvjdeapp-d","1521","JDE900D","E_IQ_CHECK_JDE_PY","CRPDTA","poc2017pw"),
List("F55BC02","dclvjdeapp-d","1521","JDE900D","E_IQ_CHECK_JDE_PY","CRPDTA","poc2017pw"),

List("ARCONTAINER","192.168.2.82","1521","ARSystem","ARAdmin","ARADMIN","AR#Admin#"),
List("REQ_HEADER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("CA_REQUEST","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("ECA_REQUEST","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("ECA_RECORD","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("PR_REQUEST","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("PR_RECORD","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("CA_REQUEST_LN_ITEM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("CA_RECORD_LN_ITEM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("ECA_REQUEST_LN_ITEM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("ECA_RECORD_LN_ITEM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("PR_REQUEST_LN_ITEM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("PR_RECORD_LN_ITEM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("SUBECA_TO_ECA_CMM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("CA_TO_ECA_CMM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("PRR_TO_ECA_CMM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("PRR_TO_CA_CMM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("CA_RECORD_LN_ITEM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("ECA_RECORD_LN_ITEM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("PR_RECORD_LN_ITEM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("REQ_HEADER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("REQ_ROUTING_ENTRY","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("SYS_BUDGET_HEAD_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("SYS_BUSINESS_UNIT_HEAD_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("SYS_BUSINESS_UNIT_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("SYS_COMMITTEE_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("SYS_COMPANY_CATEGORY_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("SYS_TENDER_TYPE_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("SYS_USER_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_ECA","abc123"),
List("BPMS_COMBINED_HEADER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_COMBINED_CMM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_COMBINED_LN_ITEM","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_COMBINED_LATEST_VERSION_LINE","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_SYS_USER_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("JDE_F0005","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("JDE_F0010","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("JDE_F0901","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("JDE_F0911","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("JDE_F550901B","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("JDE_F550901C","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("JDE_F55BC02","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("DM_FIN_EC_BUDGET_TYPE","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("DM_FIN_EC_BUDGET","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_ECA_REQUEST","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_REQ_HEADER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),

List("BPMS_REQ_ROUTING_ENTRY","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),


List("BPMS_SYS_BUDGET_HEAD_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_SYS_BUSINESS_UNIT_HEAD_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_SYS_BUSINESS_UNIT_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_SYS_COMMITTEE_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_SYS_COMPANY_CATEGORY_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_SYS_TENDER_TYPE_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123"),
List("BPMS_COMBINED_USER_MASTER","dcrhlpm","1524","eca_u","e_iq_bpms","HS_MISDM","abc123")   
);
      
      
      /*
      val tables = List(
      "HS_MISDM.DM_FIN_REF_USER_SHARED",
      "HS_MISDM.BPMS_REQ_HEADER",
      "HS_MISDM.BPMS_REQ_ROUTING_ENTRY",
      "HS_MISDM.BPMS_SYS_BUDGET_HEAD_MASTER",
      "HS_MISDM.BPMS_SYS_BUSINESS_UNIT_HEAD_MASTER",
      "HS_MISDM.BPMS_SYS_BUSINESS_UNIT_MASTER",
      "HS_MISDM.BPMS_SYS_COMMITTEE_MASTER",
      "HS_MISDM.BPMS_SYS_COMPANY_CATEGORY_MASTER",
      "HS_MISDM.BPMS_SYS_TENDER_TYPE_MASTER",
      "HS_MISDM.BPMS_COMBINED_HEADER",
      "HS_MISDM.BPMS_COMBINED_LN_ITEM",
      "HS_MISDM.BPMS_COMBINED_CMM",
      "HS_MISDM.BPMS_COMBINED_LATEST_VERSION_LINE",
      "HS_MISDM.BPMS_COMBINED_USER_MASTER",
      "HS_MISDM.JDE_F0005",
      "HS_MISDM.JDE_F0010",
      "HS_MISDM.JDE_F0901",
      "HS_MISDM.JDE_F0911",
      "HS_MISDM.JDE_F550901B",
      "HS_MISDM.JDE_F550901C",
      "HS_MISDM.JDE_F55BC02",
      "HS_MISDM.BPMS_COMBINED_USER_MASTER",
      "HS_MISDM.BPMS_SYS_SECURITY_PROFILE_MASTER",
      "hs_misdm.FIN_EC_BUDGET",
      "HS_MISDM.DM_FIN_REF_LOG",
      "HS_MISDM.DM_FIN_REF_ACCRUAL",
      "HS_MISDM.BPMS_ECA_REQUEST",
      "HS_MISDM.BPMS_SYS_USER_MASTER",
      "HS_MISDM.DM_FIN_EC_BUDGET",
      "HS_MISDM.DM_FIN_EC_BUDGET_TYPE"
      )
      */
      
      for (table <- tables)
      try{
        
        println( table );
        
        var DF = spark.read.format("jdbc")
        .option("url", "jdbc:oracle:thin:@"+ table(1) +":"+ table(2) +":"+ table(3))
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .option("dbtable", table(5)+"."+table(0))
        .option("user", table(4))
        .option("password", table(6))
        .load()
        .limit(10)

        /*
        DF.write.format("parquet")
        .mode("overwrite")
        .save("/user/mapr/data/"+ table(0) +".parquet")
        */
        println("Table " + table(0) + " saved with records count "+ DF.collect().size)
        
      }catch {
        case unknown  => println("( " + table +  ") " + unknown )
      }
            
    spark.stop()
  }
}