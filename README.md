Upstream Home Assignment Pipeline
=================================

This project implements a Bronze → Silver → Gold data pipeline using PySpark.
It reads vehicle messages from an upstream API, transforms the data, and writes
partitioned parquet files by date and hour.

Pipeline Details
----------------
1. Bronze Stage
   - Fetch raw data from API
   - Add `date` and `hour` columns from `timestamp`
   - Write partitioned parquet by date,hour `data_lake/Bronze`

2. Silver Stage
   - read data from `data_lake/Bronze`
   - Remove trailing spaces in `manufacturer`
   - Remove null VINs
   - Standardize `gearPosition` to integers

3. Gold Stage 
   - Final aggregations/metrics
   - Write results to `data_lake/Gold`



Prerequisites
-------------
- macOS, Linux 
- Conda or Miniconda
- java

Setup
-----
Create the Conda environment:
    make create


Testing and Code Quality
------------------------
    make test





