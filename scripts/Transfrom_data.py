import pandas as pd
import logging
import os
import glob

def process_Trip_Data():
    csv_files = glob.glob('./datalake/bronze/Green_trip/*.csv')
    logging.info('start for transforming data ......')

    valid_data_frames = []
    rejected_rows = []

    for file in csv_files:
        df = pd.read_csv(file)

        # Define conditions 
        df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]
        Vendor_conditions = df['VendorID'].isin([1, 2])
        Rate_condition = df['RatecodeID'].between(1,6)
        Payment_conditions = df['payment_type'].between(1,4)
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





