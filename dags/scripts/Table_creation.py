from scripts.SQL.SQL_Query import * 
from scripts.Utils.db_utils import * 

logging.basicConfig(filename='tables_creation.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def creat_tables(host,db_name,user,password):
    logging.info("Create tables ......")
    Querys = [
        (create_DimLocation,'DimLocation'),
        (create_Dimpayment,'Dimpayment'),
        (create_DimRate,'DimRate'),
        (create_DimTripTypy,"DimTrip_type"),
        (create_DimVendor,'DimVendor'),
        (create_FactTrip,'Fact_Trip')
    ]
    conn = create_connection(host,db_name,user,password)
    if conn:
        cursor = conn.cursor()
        for query,table in Querys:
            if execute_query(conn,query,table):
                logging.info(f'Table {table} created successfully')
            else:
                logging.info(f'Failed to create table {table}')
    else:
        logging.info('Failed to establish a connection to the database')


def load_to_postres(table_name,data_frame,host,db_name,user,password):
    logging.info(f'Loading data into table {table_name}...')
    conn = create_connection(host,db_name,user,password)

    if conn:
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
        # Insert new data
        Row = 0
        for i, row in data_frame.iterrows():
            cols = ', '.join(list(row.index))
            vals = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"
            cursor.execute(insert_query, tuple(row))
            Row += 1
        conn.commit()
        logging.info(f"Data loaded successfully into table {table_name} With {Row} Row")
        close_connection(conn)
    else:
        logging.error(f"Failed to connect to the database and load data into table {table_name}")