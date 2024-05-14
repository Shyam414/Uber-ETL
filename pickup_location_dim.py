import pandas as pd
import psycopg2
import connection
import uber_data  # Import the module containing pickup_location_dim

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

cur.execute("DROP TABLE IF EXISTS pickup_location_dim")
conn.commit()

# Create table query for pickup_location_dim
create_table_query = '''
CREATE TABLE pickup_location_dim (
    pickup_location_id SERIAL PRIMARY KEY,
    pickup_latitude NUMERIC,
    pickup_longitude NUMERIC
);
'''

# Execute the create table query
cur.execute(create_table_query)
conn.commit()

# Get pickup_location_dim from the uber_data module
pickup_location_dim = uber_data.pickup_location_dim

# Iterate through DataFrame rows and insert data into the table
for index, row in pickup_location_dim.iterrows():
    # Replace NaN values with None
    row = row.where(pd.notnull(row), None)
    
    insert_query = """
    INSERT INTO pickup_location_dim (
        pickup_latitude, pickup_longitude
    ) 
    VALUES (%s, %s);
    """
    cur.execute(insert_query, (row['pickup_latitude'], row['pickup_longitude']))
    conn.commit()

# Close cursor and connection
cur.close()
conn.close()
