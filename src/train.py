import sys
import logging
import findspark
from pyspark.ml import clustering, evaluation
from pyspark.sql import SparkSession
from config import config
from data_manager import DataManager
from pyspark.sql.functions import udf

logging.basicConfig(
    level=logging.INFO,  
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  
    ]
)

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    
    findspark.init()
    
    spark_session = (
    SparkSession.builder.appName(config.spark.app_name)
    .master(config.spark.deploy_mode)
    .config("spark.driver.memory", config.spark.driver_memory)
    .config("spark.executor.memory", config.spark.executor_memory)
    .config("spark.jars", 'clickhouse-jdbc-0.6.4-all.jar, clickhouse-spark-runtime-3.3_2.13-0.7.3.jar') 
    .getOrCreate()
    ) 

    logger.info('SparkSession created')
    
    data_manager = DataManager(spark_session, config.clickhouse.url, config.clickhouse.username, config.clickhouse.password)
    
    df = data_manager.read_and_preprocess(dbtable="my_db.openfood")
    
    logger.info('DataFrame created')
    
    model_args = dict(config.kmeans)
    model = clustering.KMeans(featuresCol='scaled_features', **model_args)
    model = model.fit(df)
    
    logger.info('Model fitted')
    
    evaluator = evaluation.ClusteringEvaluator(
    predictionCol="prediction",
    featuresCol='scaled_features',
    metricName="silhouette",
    distanceMeasure="squaredEuclidean",
    )
    output = model.transform(df)
    
    score = evaluator.evaluate(output)
    
    logger.info('Model evaluated')
    logger.info(f'Score: {score}')
    
    model.write().overwrite().save(config.paths.model)
    
    logger.info('Model saved')

    data_manager.overwrtie_data('predictions', output.select('code'))

    logger.info('Predictions saved')
    
    spark_session.stop()
    
    logger.info('SparkSession stopped')