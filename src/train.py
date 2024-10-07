import sys
import logging
import findspark
from pyspark.ml import clustering, evaluation
from pyspark.sql import SparkSession, DataFrame
from config import config
from datamart import DataMart

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
    
    spark = (
    SparkSession.builder.appName(config.spark.app_name)
    .master(config.spark.master_url)
    .config("spark.driver.memory", config.spark.driver_memory)
    .config("spark.executor.memory", config.spark.executor_memory)
    .config("spark.executor.cores", config.spark.executor_cores)
    .config("spark.dynamicAllocation.enabled", config.spark.dynamic_allocation)
    .config("spark.dynamicAllocation.minExecutors", config.spark.min_executors)
    .config("spark.dynamicAllocation.maxExecutors", config.spark.max_executors)
    .config("spark.dynamicAllocation.initialExecutors", config.spark.initial_executors)
    .config("spark.jars", 'clickhouse-jdbc-0.6.4-all.jar, clickhouse-spark-runtime-3.3_2.13-0.7.3.jar, datamart/target/scala-2.12/datamart_2.12-0.1.jar') 
    .getOrCreate()
    ) 

    logger.info('SparkSession created')
    
    data_manager = DataMart(spark).get_manager()
    df = data_manager.readAndPreprocess("my_db.openfood")
    df = DataFrame(df, spark)

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

    data_manager.appendData('my_db.predictions', output.select('code', 'prediction')._jdf)
    
    spark.stop()
    
    logger.info('SparkSession stopped')