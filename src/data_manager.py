import pyspark.sql
from pyspark.ml.feature import StandardScaler, VectorAssembler
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine


class DataManager:

    def __init__(self, spark_session: pyspark.sql.SparkSession, clickhouse_url: str, clickhouse_username: str, clickhouse_password: str):
        self.spark_session = spark_session
        self.clickhouse_url = clickhouse_url
        self.clickhouse_username = clickhouse_username
        self.clickhouse_password = clickhouse_password
            
            
    def read_data(self, dbtable):
        return self.spark_session.read \
            .format("jdbc") \
            .option("url", self.clickhouse_url ) \
            .option("dbtable", dbtable) \
            .option("user", self.clickhouse_username) \
            .option("password", self.clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .load()
        
        
    def overwrtie_data(self, dbtable: str, df: pyspark.sql.DataFrame):

        clickhouse_url = f'clickhouse://{self.clickhouse_username}:{self.clickhouse_password}@31.128.42.197:8123/my_db'
        engine = create_engine(clickhouse_url)
        session = make_session(engine)

        df.toPandas().to_sql(dbtable, engine, if_exists='append', index=False)

        session.close()

        # df.write.jdbc(url=self.clickhouse_url, table=dbtable, mode="overwrite",
        # properties = {
        #                 "user": self.clickhouse_username,
        #                 "password": self.clickhouse_password,
        #                 "driver": "com.clickhouse.jdbc.ClickHouseDriver"
        #                 }
        #             )

        
     
    
    
    def vectorize(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame: 
        vec_assembler = VectorAssembler(
            inputCols=['energy_kcal_100g', 
                       'energy_100g',
                       'fat_100g',
                       'saturated_fat_100g', 
                       'carbohydrates_100g',
                       'sugars_100g',
                       'fiber_100g', 
                       'proteins_100g', 
                       'salt_100g', 
                       'sodium_100g', 
                       'fruits_vegetables_nuts_estimate_from_ingredients_100g',
                       'nutrition_score_fr_100g'],
            outputCol="features"
        )
        return vec_assembler.transform(df)
    
    
    def scale(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame: 
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features").fit(df)
        return scaler.transform(df)
    
 
    def preprocess(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        
        df = self.vectorize(df)

        df = self.scale(df)
        
        return df
    

    def read_and_preprocess(self, dbtable: str) -> pyspark.sql.DataFrame:
        
        df = self.read_data(dbtable)

        df = self.preprocess(df)
        
        return df