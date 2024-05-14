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

cur.execute("DROP TABLE IF EXISTS passenger_count_dim")
conn.commit()

# Create table query
create_table_query = '''
CREATE TABLE passenger_count_dim (
    passenger_count_id SERIAL PRIMARY KEY,
    passenger_count INTEGER
);
'''

# Execute the create table query
cur.execute(create_table_query)
conn.commit()

passenger_count_dim = uber_data.passenger_count_dim


# Iterate through DataFrame rows and insert data into the table
for index, row in passenger_count_dim.iterrows():
    # Replace NaN values with None
    row = row.where(pd.notnull(row), None)
    
    insert_query = """
    INSERT INTO passenger_count_dim (passenger_count_id, passenger_count)
    VALUES (%s, %s);
    """
    cur.execute(insert_query, tuple(row))
    conn.commit()

# Close cursor and connection
cur.close()
conn.close()
