## Performance: Partition Pruning Benchmark

To validate the query optimization claims for this pipeline, I benchmarked Athena 
query performance with and without partition pruning on the Glue-cataloged S3 data.

**Setup:** Data is partitioned in S3 using Hive-style partitioning (`id=<coin>/date=<date>/hour=<hour>/`), 
auto-discovered by an AWS Glue Crawler and registered in the Glue Data Catalog as an 
external table queryable via Athena.

**Method:** Ran the same query shape twice against the `clean` table — once scanning 
all partitions, once filtered on the partition column `id`:

```sql
-- Full scan (no partition filter)
SELECT current_price FROM crypto_pipeline_db.clean;

-- Pruned scan (filtered on partition column)
SELECT current_price FROM crypto_pipeline_db.clean WHERE id = 'bitcoin';
```

**Results** (pulled from Athena's `QueryExecution.Statistics` API):

| Metric | Full scan | Pruned scan | Improvement |
|---|---|---|---|
| Data scanned | 4,926 bytes | 580 bytes | 88% reduction |
| Engine execution time | 919 ms | 652 ms | 29% faster |
| Total execution time | 1,123 ms | 874 ms | 22% faster |

**Why the gap between bytes-scanned and time improvement:** Athena queries carry fixed 
per-query overhead (queue time, query planning, service processing) that doesn't scale 
down with data volume. At this dataset's current scale, that overhead represents a larger 
proportion of total query time, which compresses the wall-clock improvement relative to 
the bytes-scanned improvement. As data volume grows toward production scale, this fixed 
overhead becomes a smaller fraction of total time, and latency improvements converge 
closer to the data-scanned reduction — which is the basis for the pipeline's partition 
strategy at scale.