ATTACH TABLE _ UUID 'd54b053d-bcc7-44ea-a24a-76cc30d326f7'
(
    `code` Int64,
    `prediction` Int64
)
ENGINE = MergeTree
ORDER BY prediction
SETTINGS index_granularity = 8192
