Upstream Home Assignment Pipeline
=================================

This project implements a data pipeline for vehicle messages
and creats Bronze , Silver , Gold & Bonus Data assets

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

3. Gold  
   - Final aggregations/metrics
   - Write results to `data_lake/Gold`

4. Bonus  
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





