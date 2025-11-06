Upstream Home Assignment Pipeline
=================================

This project implements a Bronze data pipeline using PySpark & Dagster for vehicle messages from an upstream API
,transforms the data, and create reports.

Pipeline Details
----------------
1. Bronze:
   - Fetch raw data from API
   - Add `date` and `hour` columns from `timestamp`
   - Write partitioned parquet by date,hour `data_lake/Bronze`

2. Silver:
   - read data from `data_lake/Bronze`
   - Remove trailing spaces in `manufacturer`
   - Remove null VINs
   - Standardize `gearPosition` to integers
   - Write result to `data_lake/Silver`

3. Gold Stage 
   - Final aggregations/metrics
   - Write results to `data_lake/Gold`

4. Bonus Stage 
   - create sql injection report


Prerequisites
-------------
- macOS, Linux 
- Conda or Miniconda
- java
- docker

Setup
-------------

    make create


Testing and Code Quality
------------------------

    make test





