package com.v2maestros.spark.bda.stelo;

import com.v2maestros.spark.bda.common.ExerciseUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Load {

    private static String tempDir = "file:///c:/temp/spark-warehouse";

    private static String winutils = "C:\\Spark\\winutils";

    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_CONNECTION_URL = "jdbc:oracle:thin:@10.150.25.17:1521/PRDTRNG";

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        System.setProperty("hadoop.home.dir", winutils);

        SparkSession spark = SparkSession
                .builder()
                .appName("JDBCSample")
                .config("spark.sql.warehouse.dir", tempDir)
                .master("local[2]")
                .getOrCreate();

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver");
        connectionProperties.put("user", "usr_washington_paiva");
        connectionProperties.put("password", "mudar123");

        String query = "sparkour.tif";
        query = "(select CD_PDIDO, CD_STTUS_PDIDO from USR_GEPD.TB_PDIDO where CD_ORIGEM_PRODUTO =  'M')";

        Dataset<Row> jdbcDF2 = spark.read().jdbc(ORACLE_CONNECTION_URL, query, connectionProperties);
        jdbcDF2.show(100);

        ExerciseUtils.hold();



    }
}