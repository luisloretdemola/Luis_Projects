import pandas as pd
import datetime as dt
import numpy as np
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import os


def map_query():
    
    conn = snowflake.connector.connect(account ='justworks-main',
                                            user= 'lmola@justworks.com',
                                            authenticator="externalbrowser")
    cur = conn.cursor()

    cur.execute("USE DATABASE prod_ent_db;")
    
    # Define the query
    query = """ 
    select * from PROD_ENT_DB.SALESFORCE.DISTRICT_MAPPING__C 
    """
    
    # Execute the query and load results into a DataFrame
    df = pd.read_sql(query, conn)

    # Close the connection
    conn.close()
    
    
    return df



   