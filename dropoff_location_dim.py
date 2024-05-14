import pandas as pd
import psycopg2
import uber_data  # Import the module containing dropoff_location_dim
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

cur.execute("DROP TABLE IF EXISTS dropoff_location_dim")
conn.commit()

# Create table query for dropoff_location_dim
create_table_query = '''
CREATE TABLE dropoff_location_dim (
    dropoff_location_id SERIAL PRIMARY KEY,
    dropoff_latitude FLOAT,
    dropoff_longitude FLOAT
);
'''

# Execute the create table query
cur.execute(create_table_query)
conn.commit()

# Get dropoff_location_dim from the uber_data module
dropoff_location_dim = uber_data.dropoff_location_dim

# Iterate through DataFrame rows and insert data into the table
for index, row in dropoff_location_dim.iterrows():
    # Replace NaN values with None
    row = row.where(pd.notnull(row), None)
    
    insert_query = """
    INSERT INTO dropoff_location_dim (
        dropoff_latitude, dropoff_longitude
    ) 
    VALUES (%s, %s);
    """
    cur.execute(insert_query, (row['dropoff_latitude'], row['dropoff_longitude']))
    conn.commit()

# Close cursor and connection
cur.close()
conn.close()
