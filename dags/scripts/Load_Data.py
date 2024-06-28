from Table_creation import *
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(filename='data_delivery.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# load dimvendor
def load_dimVendor(host,db_name,user,password):
    try:
        logging.info('loading Dimvendor Dimension ...')
        Dimvedor_df = pd.read_csv('./datalake/silver/Dimvendor.csv')
        load_to_postres("DimVendor",Dimvedor_df,host,db_name,user,password)
        logging.info('Dimvedor_df dimension loaded successfully')
    except Exception as E:
        logging.info(f'Error loading Dimvedor_df dimension: {str(E)}')

def load_DimLocation(host, db_name, user, password):
    try:
        logging.info('Loading location Dimension ...')
        DimLocation_df = pd.read_csv('./datalake/silver/DimLocation.csv')
        current_date = datetime.now().date()
        # Connect to the database
        conn = create_connection(host, db_name, user, password)
        if not conn:
            raise Exception("Connection to database failed")

        cursor = conn.cursor()
        
        # Load existing DimLocation table
        cursor.execute("SELECT * FROM DimLocation WHERE active_flag = 'Y'")
        existing_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        new_records = []

        for i, row in DimLocation_df.iterrows():
            # Check if the record exists and is active
            existing_record = existing_df[(existing_df['locationid'] == row['locationid']) & (existing_df['active_flag'] == 'y')]

            if not existing_record.empty:
                existing_record = existing_record.iloc[0]
                # Check if there are any changes
                if (existing_record['borough'] != row['borough'] or
                    existing_record['zone'] != row['zone'] or
                    existing_record['service_zone'] != row['service_zone']):

                    # End date the existing record
                    cursor.execute(f"""
                        UPDATE DimLocation
                        SET end_date = %s, active_flag = 'N'
                        WHERE locationid = %s AND active_flag = 'Y'
                    """, (current_date, row['locationid']))

                    # Prepare the new version of the record
                    new_records.append({
                        'locationid': row['locationid'],
                        'borough': row['borough'],
                        'zone': row['zone'],
                        'service_zone': row['service_zone'],
                        'start_date': current_date,
                        'end_date': None,
                        'active_flag': 'Y',
                        'version': existing_record['version'] + 1
                    })
            else:
                # Prepare the new record
                new_records.append({
                    'locationid': row['locationid'],
                    'borough': row['borough'],
                    'zone': row['zone'],
                    'service_zone': row['service_zone'],
                    'start_date': current_date,
                    'end_date': None,
                    'active_flag': 'Y',
                    'version': 1
                })
        
        close_connection(conn)
        # Insert all new records
        if new_records:
            new_records_df = pd.DataFrame(new_records)
            load_to_postres("DimLocation", new_records_df, host, db_name, user, password)

        logging.info('DimLocation dimension loaded successfully')
        close_connection(conn)
    except Exception as E:
        logging.info(f'Error loading location dimension: {str(E)}')


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
        Fact_df.to_csv('./datalake/gold/Fact.csv',index=False)
        logging.info('Fact_df  loaded successfully')
    except Exception as E:
        logging.info(f'Error loading  fact table: {str(E)}')
