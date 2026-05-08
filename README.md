# northwind-lakehouse-project
## Known Differences: Manual Silver vs DLT Silver

| Table | Manual | DLT | Reason |
|---|---|---|---|
| fact_orders | 13,121 | 13,245 | Streaming dedup requires `dropDuplicatesWithinWatermark` |
| Other tables | identical | identical | — |

The 124-row difference reflects a real-world tradeoff between batch and streaming pipelines:
batch supports window-ranking dedup natively, while streaming requires watermark-based 
patterns. In production, the choice depends on latency requirements and whether the source
provides idempotent IDs.