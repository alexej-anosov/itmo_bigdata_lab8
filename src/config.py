from pydantic import BaseModel
import yaml


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
    

class ConfigCreator:
    def load_config(self, file_path: str) -> Config:
        with open(file_path, 'r') as file:
            config_data = yaml.safe_load(file)
        return Config(**config_data)
        

config = ConfigCreator().load_config("../configs/config.yaml")