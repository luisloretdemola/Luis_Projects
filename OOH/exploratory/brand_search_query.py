import pandas as pd
import datetime as dt
import numpy as np
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import os


def run_brand_search_query(treatment_metro, treatment_region):
    # Establish connection to Snowflake
    conn = snowflake.connector.connect(account ='justworks-main',
                                            user= 'lmola@justworks.com',
                                            authenticator="externalbrowser")
    cur = conn.cursor()

    cur.execute("USE DATABASE prod_ent_db;")
    
    # Define the query
    brand_query = """ 
    with campaign_spend as (select state, region, metro, sum(m.cost_usd) as spend,
    sum(m.impression_count) as impressions,sum(m.click_count) as clicks,
    DATE_TRUNC('MONTH', to_date(m.metric_date)) AS month_start


    from  prod_analytics_db.marketing.fact_marketing_performance_daily_ad_metrics m 

    join prod_analytics_db.marketing.dim_marketing_performance_campaigns c

    on m.campaign_id = c.campaign_id

    join prod_analytics_db.marketing.dim_marketing_performance_ad_groups ad
    on m.ad_group_id = ad.ad_group_id

    left join (Select fiscal_year_quarter
    , r.month_name
    , r.date,day_of_week_name
    ,fiscal_week_num
    ,day_of_fiscal_quarter_num
    ,fiscal_quarter
    ,fiscal_year
    ,first_value(r.date) over(partition by iso_running_week order by r.date asc) AS week_of_qtr

    from community_db.ent_static.reference_dates r)ro

    on ro.date= m.metric_date


    where metric_date >= '2021-06-01'

    and ad.type in ('SEARCH_STANDARD')

    and (campaign_name like '%Brand%'
    or campaign_name like '%BRAND%'
    or campaign_name like '%BRD%' 
    or campaign_name like '%branded%'
    or campaign_name like '%justworks%'
    or campaign_name like '%just%')
    

    group by state, region, metro, month_start)
    --spend_month, spend_year)
    --7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    --4,5,6,7,8,9,10,11,12,13)

    select * from campaign_spend ;

    """
    
    # Execute the query and load results into a DataFrame
    df = pd.read_sql(brand_query, conn)

    # Close the connection
    conn.close()
    
    final_df = df[(df['METRO'] == treatment_metro) & (df['REGION'] == treatment_region)]

    return final_df
