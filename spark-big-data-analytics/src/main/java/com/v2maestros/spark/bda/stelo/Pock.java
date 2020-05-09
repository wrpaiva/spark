package com.v2maestros.spark.bda.stelo;

import com.v2maestros.spark.bda.apply.TIF;
import com.v2maestros.spark.bda.common.SparkConnection;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Pock {

    private static String tempDir = "file:///c:/temp/spark-warehouse";

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        System.setProperty("hadoop.home.dir", "C:\\Spark\\winutils");

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
        connectionProperties.put("dbtable", "select CD_PDIDO, CD_STTUS_PDIDO from USR_GEPD.TB_PDIDO where CD_ORIGEM_PRODUTO =  'M'");

        String url ="jdbc:oracle:thin:@10.150.25.17:1521/PRDTRNG";

        System.out.println("Carregando Lista de Pedidos : " );

        Dataset<Row> jdbcDF2 = spark.read().jdbc(url, "USR_GEPD.TB_PGTO", connectionProperties);

        jdbcDF2.show();




    }

    private static void runInferSchemaExample(SparkSession spark) {

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver");
        connectionProperties.put("user", "usr_washington_paiva");
        connectionProperties.put("password", "mudar123");
        String url ="jdbc:oracle:thin:@10.150.25.17:1521/PRDTRNG";

        spark.read().jdbc(url, "USR_GEPD.TB_PDIDO", connectionProperties);

        JavaRDD<TIF> peopleRDD = spark.read()
                .textFile("data/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    TIF tif = new TIF();
                    tif.setCodigoPedido(Long.valueOf(parts[0]));
                    tif.setStatus(parts[1].trim());
                    return tif;
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, TIF.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("select CD_PDIDO, CD_STTUS_PDIDO from USR_GEPD.TB_PDIDO where CD_ORIGEM_PRODUTO =  'M'");

        // The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Codigo: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();

        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Codigo: " + row.<String>getAs("codigo"),
                stringEncoder);
        teenagerNamesByFieldDF.show();

    }

}