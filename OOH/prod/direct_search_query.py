import pandas as pd
import datetime as dt
import numpy as np
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import os


def run_direct_search_query(treatment_city, treatment_region):
    # Establish connection to Snowflake
    conn = snowflake.connector.connect(account ='justworks-main',
                                            user= 'lmola@justworks.com',
                                            authenticator="externalbrowser")
    cur = conn.cursor()

    cur.execute("USE DATABASE prod_ent_db;")
    
    # Define the query
    direct_query = """ 
    select city, region, country, marketing_channel, DATE_TRUNC('MONTH', to_date(time)) AS month_start,
     count(distinct session_id) AS monthly_sum
    FROM PROD_ENT_DB.HEAP.SESSIONS
    where month_start >= '2021-06-01'
    and country = 'United States'
    and marketing_channel = 'Direct'
    GROUP BY city, region, country, DATE_TRUNC('MONTH', to_date(time)), marketing_channel
    ORDER BY month_start;
    """
    
    # Execute the query and load results into a DataFrame
    df = pd.read_sql(direct_query, conn)

    # Close the connection
    conn.close()
    
    final_df = df[(df['CITY'] == treatment_city) & (df['REGION'] == treatment_region)]

    return final_df

