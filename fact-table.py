import pandas as pd
import psycopg2
import uber_data  # Import the module containing fact_table
import connection

# Define connection parameters
conn_params = {
    "host": "127.0.0.2",
    "port": "5432",
    "user": "postgres",
    "password": input("Enter your database password: "),
    "database": "postgres"
}



# Establish connection
conn = psycopg2.connect(**conn_params)

# Create cursor
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS fact")
conn.commit()

# Create table query
create_table_query = '''
CREATE TABLE fact (
    trip_id SERIAL PRIMARY KEY,
    VendorID INTEGER,
    datetime_id INTEGER,
    passenger_count_id INTEGER,
    trip_distance_id INTEGER,
    rate_code_id INTEGER,
    store_and_fwd_flag TEXT,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    payment_type_id INTEGER,
    fare_amount NUMERIC,
    extra NUMERIC,
    mta_tax NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    improvement_surcharge NUMERIC,
    total_amount NUMERIC
);
'''

# Execute the create table query
cur.execute(create_table_query)
conn.commit()

# Get fact_table from the uber_data module
fact_table = uber_data.fact_table

# Iterate through DataFrame rows and insert data into the table

for index, row in fact_table.iterrows():
    # Replace NaN values with None
    row = row.where(pd.notnull(row), None)
    
    insert_query = """
    INSERT INTO fact (
        trip_id, VendorID, datetime_id, passenger_count_id, trip_distance_id,
        rate_code_id, store_and_fwd_flag, pickup_location_id, dropoff_location_id,
        payment_type_id, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.execute(insert_query, tuple(row))
    conn.commit()


# Close cursor and connection
cur.close()
conn.close()