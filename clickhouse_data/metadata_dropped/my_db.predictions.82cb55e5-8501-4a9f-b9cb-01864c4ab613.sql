ATTACH TABLE _ UUID '82cb55e5-8501-4a9f-b9cb-01864c4ab613'
(
    `code` Int64,
    `prediction` Int64
)
ENGINE = MergeTree
ORDER BY prediction
SETTINGS index_granularity = 8192
