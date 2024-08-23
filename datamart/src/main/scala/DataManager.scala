import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.sql.functions.col
import java.sql.{Connection, DriverManager, Statement}

class DataManager(sparkSession: SparkSession, clickhouseUrl: String, clickhouseUsername: String, clickhousePassword: String) {
  
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


def dropAndCreateTable(dbtable: String, df: DataFrame): Unit = {
  val connection: Connection = DriverManager.getConnection(clickhouseUrl, clickhouseUsername, clickhousePassword)
  val statement: Statement = connection.createStatement()

  try {
    statement.execute(s"DROP TABLE IF EXISTS $dbtable")

    val createTableSQL = s"""
      CREATE TABLE $dbtable (
        code Int64,
        prediction Int64
    ) ENGINE = MergeTree()
    ORDER BY prediction
    """
    
    statement.execute(createTableSQL)

  } finally {
    statement.close()
    connection.close()
  }
}


  def overwriteData(dbtable: String, df: DataFrame): Unit = {
    dropAndCreateTable(dbtable, df)

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