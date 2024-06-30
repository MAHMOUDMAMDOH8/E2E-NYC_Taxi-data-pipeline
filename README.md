
# Comprehensive NYC Taxi trip Data Pipeline

The pipeline should combine generated trip data with data from external sources, perform data
transformations and aggregations, and store the final dataset in a database. 
The aim is to enable analysis and derive insights into customer behaviour .

# Tech Stack & Tools

Infrastructure: Docker

Data Warehouse: PostgreSQL

Database: PostgreSQL

Orchestration: Apache Airflow

Serving Layer: PowerBI

## Table of Contents
- [Introduction](#introduction)
- [Database Schema](#database-schema)
- [Reporting](#reporting-layer)

## Introduction
The goal of this project is to build an end-to-end data pipeline for NYC Taxi trip data, transforming it, and storing it in a data warehouse. Throughout the planning process, various approaches were considered to tackle emerging challenges. 
Initially, a straightforward ETL job was considered, but the project was made more challenging and modern by adopting the Medallion architecture. 
This architecture incrementally and progressively improves the structure and quality of data as it flows through each layer (from Bronze ⇒ Silver ⇒ Gold).

![architecture](https://github.com/MAHMOUDMAMDOH8/E2E-NYC_Taxi-data-pipeline/assets/111503676/96381be5-097a-4cd5-b448-c6cc80282780)

### Assumptions
- The nyc trip data is assumed to be clean and does not require extensive cleaning or preprocessing.
- The database schema follows a star schema design for efficient querying and analysis.
### Step 1: Data Collection and Storage
We have  one Data Sources:
  -nyc trip  Data: Stored in CSV format. 
transform_data.py:
 -  Data is initially stored in a data lake in the "bronze" layer (raw data) in csv format to have unified landing zone.
 -  process_Ecommrese_data()
 -     - get data from bronze layear and  Removes duplicate entries and git dimensions and loade to silver layer
### Step 2: Data Delivery
 - tables_creation.py:
    - Contains functions to create, drop, and load tables, reading the statements from sql_queries.py for preparing the data warehouse.
 - load_dimVendor():
        - load transformed vendor data from  bronze layer, selecting required columns and loaded into a PostgreSQL database in the Gold layerles_dim()
        - Load transformed sales data from silver layer, selecting required columns and loaded into a PostgreSQL database in the Gold layer
 - load_DimLocation() :
        -  load transformed  location data from  silver layer, selecting required columns and loaded into a PostgreSQL database in the Gold layerles_dim()
        -  Load transformed sales data from silver layer, selecting required columns and loaded into a PostgreSQL database in the Gold layer
 - load_DimRate():
        -  Extracts and cleans rate data from the Silver layer,and loaded into a PostgreSQL database in the Gold layerles_dim()
 - load_DimPayment():
        -  Extracts and cleans payment data from the Silver layer,and loaded into a PostgreSQL database in the Gold layerles_dim()
 - Load_DimTrip_type():
        -  Extracts and cleans Trip_type data from the Silver layer,and loaded into a PostgreSQL database in the Gold layerles_dim()
 - load_fact():
        - load transformed customer data from  silver layer, selecting required columns and loaded into a PostgreSQL database in the Gold layerles_dim()
   
  db_utils.py
    - The `db_utils.py` module provides a set of database utility functions for interacting with the PostgreSQL database. 
    - These functions handle database connections, data loading, and query execution, ensuring efficient and reliable data management within the pipeline.

## Database Schema 
The transformed data is stored in a relational database with a star schema design, consisting of the following tables:

1 Dim_vendor : Contains information about vendor id and vendor name

2 Dim_rate : Contains information about rate id and rate name 

3 dim_location : Contains information about location id , zone , Borough and service_zone 

4 Dim_payment : Contains information about payment id and type

5 dim_trip : Contains information about trip id and type

6 Fact_trip : Contains information about VendorID  , lpep_pickup_datetime , lpep_dropoff_datetime , store_and_fwd_flag , RatecodeID  , PULocationID , DOLocationID  , passenger_count , trip_distance , fare_amount , extra , mta_tax , tip_amount , tolls_amount , ehail_fee , improvement_surcharge , total_amount , payment_type , trip_type and congestion_surcharge

![model](https://github.com/MAHMOUDMAMDOH8/E2E-NYC_Taxi-data-pipeline/assets/111503676/649b3dba-1fcd-47e6-8b6a-2a9f67d65be9)

## Reporting layer

![overview](https://github.com/MAHMOUDMAMDOH8/E2E-NYC_Taxi-data-pipeline/assets/111503676/baa3e122-0281-4242-a803-72b68d4bb857)

![zone](https://github.com/MAHMOUDMAMDOH8/E2E-NYC_Taxi-data-pipeline/assets/111503676/5aa52697-a4cc-43b4-b48a-95d7e6274546)


