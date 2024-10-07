import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import java.sql.{Connection, DriverManager, Statement}
import sys.process._


class DataMart {
    private val appName = "DataMartApp"
    private val masterUrl = "spark://spark:7077"
    private val driverMemory = "1g"
    private val executorMemory = "1g"
    private val executorCores = 1
    private val driverCores = 1
    private val clickhouseUrl: String = sys.env("CLICKHOUSE_URL")
    private val clickhouseUsername: String = sys.env("CLICKHOUSE_USER")
    private val clickhousePassword: String = sys.env("CLICKHOUSE_PASSWORD")
    private val dynamicAllocation = false
    private val minExecutors = 1
    private val maxExecutors = 1
    private val initialExecutors = 1
    
    private val sparkSession: SparkSession = SparkSession.builder()
      .appName(appName)
      .master(masterUrl)
      .config("spark.driver.cores", driverCores)
      .config("spark.executor.cores", executorCores)
      .config("spark.driver.memory", driverMemory)
      .config("spark.executor.memory", executorMemory)
      .config("spark.dynamicAllocation.enabled", dynamicAllocation)
      .config("spark.dynamicAllocation.minExecutors", minExecutors)
      .config("spark.dynamicAllocation.maxExecutors", maxExecutors)
      .config("spark.dynamicAllocation.initialExecutors", initialExecutors)
      .config("spark.jars", "clickhouse-jdbc-0.6.4-all.jar, clickhouse-spark-runtime-3.3_2.13-0.7.3.jar")
      .getOrCreate()
  
  def readData(dbtable: String): DataFrame = {
    sparkSession.read
      .format("jdbc")
      .option("url", clickhouseUrl)
      .option("dbtable", dbtable)
      .option("user", clickhouseUsername)
      .option("password", clickhousePassword)
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .load()
  }

  def vectorize(df: DataFrame): DataFrame = {
    val vecAssembler = new VectorAssembler()
      .setInputCols(Array(
        "energy_kcal_100g",
        "energy_100g",
        "fat_100g",
        "saturated_fat_100g",
        "carbohydrates_100g",
        "sugars_100g",
        "fiber_100g",
        "proteins_100g",
        "salt_100g",
        "sodium_100g",
        "fruits_vegetables_nuts_estimate_from_ingredients_100g",
        "nutrition_score_fr_100g"
      ))
      .setOutputCol("features")

    vecAssembler.transform(df)
  }

  def scale(df: DataFrame): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .fit(df)

    scaler.transform(df)
  }

  def preprocess(df: DataFrame): DataFrame = {
    val vectorizedDF = vectorize(df)
    scale(vectorizedDF)
  }

  def readAndPreprocess(dbtable: String): DataFrame = {
    val df = readData(dbtable)
    preprocess(df)
  }

  def appendData(dbtable: String, df: DataFrame): Unit = {

    df.write
      .format("jdbc")
      .option("url", clickhouseUrl)
      .option("dbtable", dbtable)
      .option("user", clickhouseUsername)
      .option("password", clickhousePassword)
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .mode("append") 
      .save()
  }
}
