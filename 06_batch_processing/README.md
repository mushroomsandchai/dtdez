# Week 6 – Batch Processing

This module covers the fundamentals and practical applications of **batch processing** in distributed computing environments, with a primary focus on **Apache Spark**.

## Overview

Batch processing is a method of executing a series of jobs on large datasets without user interaction. Unlike real-time processing, batch jobs process data in chunks at scheduled intervals, making them ideal for:

- Large-scale data transformations
- Complex analytical queries
- Cost-effective resource utilization
- Non-time-critical data processing pipelines


## Learning Resources

This folder contains visual notes and diagrams covering key concepts:

```
notes/
├── 01_introduction_to_batch_processing.png
├── 02_introduction_to_spark.png
├── 03.01_spark_internals.png
├── 03.02_anatomy_of_a_spark_cluster.png
├── 03.03_groupby_in_spark.png
└── 03.04_joins_in_spark.png
```

## Key Concepts

### Apache Spark Architecture
- **Driver**: Coordinates job execution
- **Executors**: Perform computations on worker nodes
- **Cluster Manager**: Allocates resources (YARN, Kubernetes, Standalone)

### Data Processing Models
- **Transformations**: Lazy operations (map, filter, join, etc.)
- **Actions**: Trigger execution (collect, save, count, etc.)

### Optimization Techniques
- Selecting appropriate join strategies
- Partitioning data efficiently
- Broadcasting small tables
- Avoiding unnecessary shuffle operations

## References

- [Data Engineering Zoomcamp by DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/06-batch)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)