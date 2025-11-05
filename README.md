Upstream Home Assignment Pipeline
=================================

This project implements a Bronze data pipeline using PySpark for vehicle messages from an upstream API
,transforms the data, and create reports.

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





