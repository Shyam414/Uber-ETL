import pandas as pd
import psycopg2
import uber_data  # Import the module containing fact_table

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

cur.execute("DROP TABLE IF EXISTS datetime_dim")
conn.commit()

# Create table query
create_table_query = '''
    CREATE TABLE datetime_dim (
        datetime_id SERIAL PRIMARY KEY,
        tpep_pickup_datetime TIMESTAMP,
        pick_hour INT,
        pick_day INT,
        pick_month INT,
        pick_year INT,
        pick_weekday INT,
        tpep_dropoff_datetime TIMESTAMP,
        drop_hour INT,
        drop_day INT,
        drop_month INT,
        drop_year INT,
        drop_weekday INT
    );
    '''

# Execute the create table query
cur.execute(create_table_query)
conn.commit()

# Get fact_table from the uber_data module
datetime_dim = uber_data.datetime_dim

# Iterate through DataFrame rows and insert data into the table
for index, row in datetime_dim.iterrows():
    # Replace NaN values with None
    row = row.where(pd.notnull(row), None)
    
    insert_query = """
    INSERT INTO datetime_dim (
        datetime_id, tpep_pickup_datetime, pick_hour, pick_day, pick_month,
        pick_year, pick_weekday,tpep_dropoff_datetime, drop_hour,
         drop_day, drop_month, drop_year, drop_weekday
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.execute(insert_query, tuple(row))
    conn.commit()

# Close cursor and connection
cur.close()
conn.close()