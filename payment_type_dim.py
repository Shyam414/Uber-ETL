import pandas as pd
import psycopg2
import uber_data
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
cur.execute("DROP TABLE IF EXISTS payment_type_dim")
conn.commit()
# Create table query for payment_type_dim
create_table_query = '''
CREATE TABLE payment_type_dim (
    payment_type_id SERIAL PRIMARY KEY,
    payment_type INTEGER,
    payment_type_name TEXT
);
'''

# Execute the create table query
cur.execute(create_table_query)
conn.commit()

payment_type_dim = uber_data.payment_type_dim




# Iterate through payment type data and insert into the table
for index, row in payment_type_dim.iterrows():
    # Replace NaN values with None
    row = row.where(pd.notnull(row), None)
    
    insert_query = """
    INSERT INTO payment_type_dim (
        payment_type_id ,
        payment_type,
        payment_type_name
    ) 
    VALUES (%s, %s, %s);
    """
    cur.execute(insert_query, tuple(row))
    conn.commit()

# Close cursor and connection
cur.close()
conn.close()
