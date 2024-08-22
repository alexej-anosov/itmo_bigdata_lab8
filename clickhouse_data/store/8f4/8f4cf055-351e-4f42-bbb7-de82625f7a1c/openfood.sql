ATTACH TABLE _ UUID '2a32a22f-aeb1-4d30-89c2-77bcee952a4f'
(
    `code` Int64,
    `product_name` String,
    `created_t` DateTime,
    `last_modified_t` DateTime,
    `last_updated_t` DateTime,
    `serving_quantity` Float64,
    `additives_n` Float64,
    `nutriscore_score` Float64,
    `nova_group` Float64,
    `completeness` Float64,
    `last_image_t` DateTime,
    `energy_kcal_100g` Float64,
    `energy_100g` Float64,
    `fat_100g` Float64,
    `saturated_fat_100g` Float64,
    `carbohydrates_100g` Float64,
    `sugars_100g` Float64,
    `fiber_100g` Float64,
    `proteins_100g` Float64,
    `salt_100g` Float64,
    `sodium_100g` Float64,
    `fruits_vegetables_nuts_estimate_from_ingredients_100g` Float64,
    `nutrition_score_fr_100g` Float64
)
ENGINE = MergeTree
ORDER BY code
SETTINGS index_granularity = 8192
