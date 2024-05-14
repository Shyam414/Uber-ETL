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

cur.execute("DROP TABLE IF EXISTS trip_distance_dim")
conn.commit()

# Define the create table query for trip_distance_dim
create_trip_distance_dim_table_query = '''
CREATE TABLE trip_distance_dim (
    trip_distance_id SERIAL PRIMARY KEY,
    trip_distance NUMERIC
);
'''

# Execute the create table query for trip_distance_dim
cur.execute(create_trip_distance_dim_table_query)
conn.commit()

# Get the trip_distance_dim DataFrame from the uber_data module
trip_distance_dim = uber_data.trip_distance_dim

# Iterate through DataFrame rows and insert data into the table
for index, row in trip_distance_dim.iterrows():
    # Replace NaN values with None
    row = row.where(pd.notnull(row), None)
    
    insert_trip_distance_dim_query = """
    INSERT INTO trip_distance_dim (trip_distance_id, trip_distance) 
    VALUES (%s, %s);
    """
    cur.execute(insert_trip_distance_dim_query, (row['trip_distance_id'], row['trip_distance']))
    conn.commit()

# Close cursor and connection
cur.close()
conn.close()
