import os
import sys
import logging
import findspark
from pyspark.ml import clustering, evaluation
from pyspark.sql import SparkSession

from config import config
from preprocess import Preprocessor


os.environ['SPARK_HOME'] = "/root/spark"
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-21-openjdk-amd64'


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
    .getOrCreate()
    ) 
    
    logger.info('SparkSession created')
    
    preprocessor = Preprocessor(spark_session, config.paths.data)
    df = preprocessor.create_df()

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
    
    spark_session.stop()
    
    logger.info('SparkSession stopped')