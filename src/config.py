from pydantic import BaseModel
import yaml
import os
from dotenv import load_dotenv


load_dotenv(override=True)


class ClickhouseConfig(BaseModel):
    username: str
    password: str
    url: str 


class SparkSessionConfig(BaseModel):
    app_name: str
    deploy_mode: str
    driver_memory: str
    executor_memory: str
    
    
class PathsConfig(BaseModel):
    data: str
    model: str
    
    
class KMeansConfig(BaseModel):
    k: int
    maxIter: int
    seed: int

    
class Config(BaseModel):
    spark: SparkSessionConfig
    paths: PathsConfig
    kmeans: KMeansConfig
    clickhouse: ClickhouseConfig
    

class ConfigCreator:
    def load_config(self, file_path: str) -> Config:
        with open(file_path, 'r') as file:
            config_data = yaml.safe_load(file)
            
        config_data['clickhouse'] = {'username': os.getenv('CLICKHOUSE_USER'), 'password': os.getenv('CLICKHOUSE_PASSWORD'), 'url': os.getenv('CLICKHOUSE_URL')}
        
        return Config(**config_data)
        

config = ConfigCreator().load_config("configs/config.yaml")