ATTACH TABLE _ UUID '029417f7-c25b-475f-8184-c5eee7e49101'
(
    `code` Int64,
    `prediction` Int64
)
ENGINE = MergeTree
ORDER BY prediction
SETTINGS index_granularity = 8192
