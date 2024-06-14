from Table_creation import *
import pandas as pd
import logging 
import datetime as datetime

logging.basicConfig(filename='data_delivery.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_dimVendor(host,db_name,user,password):
    try:
        logging.info('loading vendor Dimension ...')
        vendor_df = pd.read_csv('./datalake/silver/Dimvendor.csv')
        load_to_postres("DimVendor",vendor_df,host,db_name,user,password)
        logging.info('vendor_df dimension loaded successfully')
    except Exception as E:
        logging.info(f'Error loading vendor dimension: {str(E)}')

def load_DimLocation(host,db_name,user,password):
    try:
        logging.info('loading location Dimension ...')
        DimLocation_df = pd.read_csv('./datalake/silver/DimLocation.csv')
        load_to_postres("DimLocation",DimLocation_df,host,db_name,user,password)
        logging.info('DimLocation_df dimension loaded successfully')
    except Exception as E:
        logging.info(f'Error loading Location dimension: {str(E)}')

def load_DimRate(host,db_name,user,password):
    try:
        logging.info('loading DimRate Dimension ...')
        DimRate_df = pd.read_csv('./datalake/silver/Dimrater.csv')
        load_to_postres("DimRate",DimRate_df,host,db_name,user,password)
        logging.info('DimRate_df dimension loaded successfully')
    except Exception as E:
        logging.info(f'Error loading DimRate dimension: {str(E)}')

def Load_DimTrip_type(host,db_name,user,password):
    try:
        logging.info('loading trip_type dimension....')
        trip_df = pd.read_csv('./datalake/silver/DIM_TripType.csv')
        load_to_postres('DimTrip_type',trip_df,host,db_name,user,password)
        logging.info('trip_df dimension loaded successfully')
    except Exception as E:
        logging.info(f'Error loading trip dimension: {str(E)}')

def load_DimPayment(host,db_name,user,password):
    try:
        logging.info('loading payment dimension....')
        payment_df = pd.read_csv('./datalake/silver/DIM_payment.csv')
        load_to_postres('DimPayment',payment_df,host,db_name,user,password)
        logging.info('payment_df dimension loaded successfully')
    except Exception as E:
        logging.info(f'Error loading payment dimension: {str(E)}')

def load_fact(host,db_name,user,password):
    try:
        logging.info('loading fact table ....')
        Taxi_Dtype = {
            'VendorID': pd.Int64Dtype(),
            'passenger_count': pd.Int64Dtype(),
            'trip_distance': float,
            'RatecodeID': pd.Int64Dtype(),
            'store_and_fwd_flag': str,
            'PULocationID': pd.Int64Dtype(),
            'DOLocationID': pd.Int64Dtype(),
            'payment_type': pd.Int64Dtype(),
            'fare_amount': float,
            'extra': float,
            'mta_tax': float,
            'tip_amount': float,
            'tolls_amount': float,
            'improvement_surcharge': float,
            'total_amount': float,
            'congestion_surcharge': float
        }
        Fact_df = pd.read_csv('./datalake/silver/FactTrip.csv',dtype=Taxi_Dtype)
        load_to_postres('Fact_Trip',Fact_df,host,db_name,user,password)
        Fact_df.to_csv('./datalake/gold/FactTrip.csv',index=False)
        logging.info('Fact_df  loaded successfully')
    except Exception as E:
        logging.info(f'Error loading  fact table: {str(E)}')
