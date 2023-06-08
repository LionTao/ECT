# strajdb-thesis
Real-time distributed trajectory distance calculation system based on elastic load

## Project structure

| Folder                   | Description                    |
|--------------------------|--------------------------------|
| [Baselines](./baselines) | DITA and GeoFlink as baselines |
| [strajdb](./strajdb)     | Our solution                   |

## Strajdb (Stream trajectory database)

Our solution is based on [Dapr](https://dapr.io). 
Most of our components are built with dapr actor.

Strajdb can be devided into three layers: 
- **Query layer**
- **Index layer**
- **Storage layer**.

### Query layer

This layer is responsible for data ingestion and indexing. It contains two major modules:
- Assembler: trajectory segmentation
- Grid index: distributed index for trajectory segments

### Query layer

This layer is responsible for process user queries. It contains two major modules:
- Agent: gateway for query preprocessing
- Distance compute: trajectory distance computation with enhanced stream trajectory distance algorithms

### Storage layer

This layer is responsible for data storage and data retrieval.
The storage backend is S3-like object storage with alluxio as in-memory cache.
It contains two major modules:
- MOR: merge on read, merge small csv segment files into large compressed parquet file.
- Pushdown: using hints in query predicate to filter out unnecessary data before leave storage layer to minimize data transfer.


## Module structure

| Name       | Folder                                       | Language | Actor |
|------------|----------------------------------------------|----------|-------|
| Assembler  | [strajdb-assembler](./strajdb/assembler)     | Java     | ✅     |
| Grid index | [strajdb-grid](./strajdb/grid)               | Java     | ✅     |
| Agent      | [strajdb-query](./strajdb/query)             | Java     | ✅     |
| Compute    | [strajdb-compute](./strajdb/stream-distance) | Python3  | ✅     |
| MOR        | [strajdb-mor](./strajdb/mor)                 | Java     | ✅     |
| Pushdown   | [strajdb-pushdown](./strajdb/pushdown)       | Java     | ✅     |

