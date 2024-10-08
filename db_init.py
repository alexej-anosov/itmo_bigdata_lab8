from clickhouse_driver import Client
import pandas as pd
from src.config import config


csv_file_path = 'data/subset.csv'
        

client = Client(host='192.168.49.2', port='30001',
                user=config.clickhouse.username,       
                password=config.clickhouse.password   
        )


client.execute('CREATE DATABASE IF NOT EXISTS my_db')


client.execute('''
    CREATE TABLE IF NOT EXISTS my_db.openfood (
        code Int64,
        product_name String,
        created_t DateTime,
        last_modified_t DateTime,
        last_updated_t DateTime,
        serving_quantity Float64,
        additives_n Float64,
        nutriscore_score Float64,
        nova_group Float64,
        completeness Float64,
        last_image_t DateTime,
        energy_kcal_100g Float64,
        energy_100g Float64,
        fat_100g Float64,
        saturated_fat_100g Float64,
        carbohydrates_100g Float64,
        sugars_100g Float64,
        fiber_100g Float64,
        proteins_100g Float64,
        salt_100g Float64,
        sodium_100g Float64,
        fruits_vegetables_nuts_estimate_from_ingredients_100g Float64,
        nutrition_score_fr_100g Float64
    ) ENGINE = MergeTree()
    ORDER BY code
''')

client.execute('''
    CREATE TABLE IF NOT EXISTS my_db.predictions (
        code Int64,
        prediction Int64
    ) ENGINE = MergeTree()
    ORDER BY prediction
''')

row_count = client.execute('SELECT count() FROM my_db.openfood')[0][0]


if row_count == 0:

    df = pd.read_csv(csv_file_path)
    df['created_t'] = pd.to_datetime(df['created_t'])
    df['last_modified_t'] = pd.to_datetime(df['last_modified_t'])
    df['last_updated_t'] = pd.to_datetime(df['last_updated_t'])
    df['last_image_t'] = pd.to_datetime(df['last_image_t'])
    data = list(df.itertuples(index=False))
    
    client.execute('INSERT INTO my_db.openfood VALUES', data, types_check=True)