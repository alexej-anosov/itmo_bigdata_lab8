import pyspark.sql
from pyspark.ml.feature import StandardScaler, VectorAssembler


class Preprocessor:
    def __init__(self, spark_session: pyspark.sql.SparkSession, data_path: str):
        self.data_path = data_path
        self.spark_session = spark_session
        self.df = None


    def load_data(self):
        self.df = self.spark_session.read.csv(self.data_path, header=True, inferSchema=True)


    def vectorize(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:  
        vec_assembler = VectorAssembler(
            inputCols=df.columns, outputCol="features"
        )
        return vec_assembler.transform(df)
    
    
    def scale(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame: 
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features").fit(df)
        return scaler.transform(df)
    
    
    def create_df(self) -> pyspark.sql.DataFrame:
        
        self.load_data()

        self.df = self.df.drop('code', 'product_name')

        self.df = self.vectorize(self.df)

        self.df = self.scale(self.df)
        
        return self.df