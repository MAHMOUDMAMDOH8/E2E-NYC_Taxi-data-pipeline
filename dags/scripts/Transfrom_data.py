import pandas as pd
import logging
import os
import glob

def process_Trip_Data():
    csv_files = glob.glob('./datalake/bronze/Green_trip/*.csv')
    logging.info('start for transforming data ......')

    valid_data_frames = []
    rejected_rows = []
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

    for file in csv_files:
        df = pd.read_csv(file,dtype=Taxi_Dtype)

        # Define conditions 
        df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]
        Vendor_conditions = df['VendorID'].isin([1, 2])
        Rate_condition = df['RatecodeID'].isin([1, 2, 3, 4, 5, 6])
        Payment_conditions = df['payment_type'].isin([1, 2, 3, 4])
        Trip_conditions = df['trip_type'].isin([1, 2])
        PULLocation_conditions = df['PULocationID'].between(1, 265)
        dropLocation_conditions = df['DOLocationID'].between(1, 265)

        valid_rows = Vendor_conditions & Rate_condition & Payment_conditions & Trip_conditions & PULLocation_conditions & dropLocation_conditions

        rejected = df[~valid_rows]
        accepted = df[valid_rows]
        
        if not accepted.empty:
            valid_data_frames.append(accepted)
        
        if not rejected.empty:
            rejected_rows.append(rejected)

    # Ensure the directories exist
    os.makedirs('./datalake/silver/rejected_Data', exist_ok=True)
    os.makedirs('./datalake/silver', exist_ok=True)

    # Concatenate and save rejected rows
    if rejected_rows:
        rejected_df = pd.concat(rejected_rows, ignore_index=True)
        rejected_df.to_csv('./datalake/silver/rejected_Data/rejected_rows.csv', index=False)
        logging.info('Storing of rejected_df completed successfully')

    # Concatenate and save valid data frames
    if valid_data_frames:
        valid_df = pd.concat(valid_data_frames, ignore_index=True)
        valid_df.to_csv('./datalake/silver/FactTrip.csv', index=False)
        logging.info('Storing of valid_df completed successfully')

    logging.info('Process of trip data completed successfully')





