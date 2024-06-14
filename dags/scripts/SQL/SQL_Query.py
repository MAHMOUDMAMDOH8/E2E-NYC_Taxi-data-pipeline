create_DimVendor = """
CREATE TABLE IF NOT EXISTS DimVendor(
        vendor_id int primary key,
        vendor_name varchar(225)
    )
"""

create_DimTripTypy = """
CREATE TABLE IF NOT EXISTS DimTrip_type(
        Trip_type_ID int primary key,
        type varchar(225)
)
"""

create_Dimpayment = """
CREATE TABLE IF NOT EXISTS DimPayment(
        Payment_ID int primary key ,
        Payment_type varchar(225)
)
"""

create_DimLocation = """
CREATE TABLE IF NOT EXISTS DimLocation(
        LocationID int primary key ,
        Borough varchar(225) ,
        Zone varchar(225),
        service_zone varchar(225)
)
"""

create_DimRate = """
CREATE TABLE IF NOT EXISTS DimRate(
        RatecodeID int primary key,
        Rate_name varchar(225)
)
"""

create_FactTrip = """
CREATE TABLE IF NOT EXISTS Fact_Trip(
        VendorID int ,
        lpep_pickup_datetime date,
        lpep_dropoff_datetime date,
        store_and_fwd_flag varchar(10),
        RatecodeID int ,
        PULocationID int,
        DOLocationID int ,
        passenger_count int,
        trip_distance float,
        fare_amount float,
        extra float,
        mta_tax float,
        tip_amount float,
        tolls_amount float,
        ehail_fee varchar(10),
        improvement_surcharge float,
        total_amount float,
        payment_type int,
        trip_type int,
        congestion_surcharge float
)
"""