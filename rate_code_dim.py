import pandas as pd
import psycopg2
import uber_data  # Import the module containing rate_code_dim
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

cur.execute("DROP TABLE IF EXISTS rate_code_dim")
conn.commit()

# Create table query for rate_code_dim
create_rate_code_dim_query = '''
CREATE TABLE rate_code_dim (
    rate_code_id SERIAL PRIMARY KEY,
    RatecodeID INTEGER,
    rate_code_name TEXT
);
'''

# Execute the create table query for rate_code_dim
cur.execute(create_rate_code_dim_query)
conn.commit()

# Get rate_code_dim from the uber_data module
rate_code_dim = uber_data.rate_code_dim

# Iterate through DataFrame rows and insert data into the rate_code_dim table
for index, row in rate_code_dim.iterrows():
    # Replace NaN values with None
    row = row.where(pd.notnull(row), None)
    
    insert_query = """
    INSERT INTO rate_code_dim ( rate_code_id,
    RatecodeID,
    rate_code_name) 
    VALUES (%s, %s,%s);
    """
    cur.execute(insert_query, tuple(row))

    conn.commit()

# Close cursor and connection
cur.close()
conn.close()
