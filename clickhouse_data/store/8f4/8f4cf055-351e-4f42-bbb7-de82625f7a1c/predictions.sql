ATTACH TABLE _ UUID 'aa3dc1bb-d060-47f9-b193-b74bd9411500'
(
    `code` Int64,
    `prediction` Int64
)
ENGINE = MergeTree
ORDER BY prediction
SETTINGS index_granularity = 8192
