import pandas as pd
import datetime as dt
import numpy as np
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import os


def run_snowflake_query():
    # Establish connection to Snowflake
    conn = snowflake.connector.connect(account ='justworks-main',
                                            user= 'lmola@justworks.com',
                                            authenticator="externalbrowser")
    cur = conn.cursor()

    cur.execute("USE DATABASE prod_ent_db;")
    
    # Define the query
    io_query = """ with IOs as (

    select distinct *

    from (
        select
            o.id as ios,
            o.accountid as account_id,
            a.id as id_frm_accts,
            a.billingcity, 
            a.billingstate, 
            a.billingcountry,
            a.BILLING_ZIP_CLEAN__C as billingzip,
            fiscal_year_quarter as IO_yyyyqq,
            week_of_qtr as IO_week_of_qtr,
            fiscal_quarter as IO_quarter,
            fiscal_year as IO_year,
            month_name as IO_month,
            rq.iso_year_month as io_year_month,
            o.name as opportunity_name,
            o.opp_source__c,
            o.state__c as state,
            o.stagename,
            us.name as owner_name,
            o.closed_lost_why__c as DQ_or_closed_lost_reason,
            o.ee__c as io_lives,
            o.type as opportunity_type,
            il.industry_category_broad as industry,
            a.regional_district__c as region,
            l.utm_campaign__c,
            l.utm_content__c,
            l.utm_term__c,
            l.utm_keyword__c,
            to_date(date_interested__c) as io_date,
            case
                when lower(o.routed_to_vertx__c) like '%yes%' then 'Vertex'
                else 'Non Vertex'
            end as is_vertex,
            case
                when o.ee__c < 7 then '< 7'
                when o.ee__c between 7 and 15 then '7 - 15'
                when o.ee__c between 16 and 49 then '16 - 49'
                when o.ee__c >= 50 then '50 +' else 'Other'
            end as employee_count_grouping,
            case
                when o.core_product__c like '%Payroll%' then 'Payroll' when
                    o.core_product__c like '%International%'
                    then 'International'
                else 'PEO'
            end as core_product__c,
            coalesce (lower(
                coalesce(l.unify_tag__c, c.unify_tag__c)
            ) is not null,
            false) as unify,
            case
                when
                    lower(
                        coalesce(l.unify_tag__c, c.unify_tag__c)
                    ) like '%new record%'
                    then 'New Record'
                when
                    lower(
                        coalesce(l.unify_tag__c, c.unify_tag__c)
                    ) like '%updated%'
                    then 'Updated Record'
            end as unify_record_type,
            coalesce (coalesce(l.leadsource, c.leadsource) in ('Event'),
            false) as event,
            coalesce(l.leadsource, c.leadsource) as leadsource,
            coalesce(l.landing_page__c, c.landing_page__c) as landing_page,
            coalesce(l.lead_source_detail__c, c.lead_source_detail__c)
                as leadsource_detail,
            case
                when (
                    (opp_source__c is null or opp_source__c like '%Other%')
                    and uro.name not like '%Self Service%'
                )
                or
                (
                    core_product__c like '%Payroll%'
                    and us.name like '%Ryan Stani%'
                    or us.name like '%Sapan P%'
                )
                    then 'Exclude'
                else 'Include'
            end as Inbound_Exclusion_Flag,
            case
                when uro.name like '%Self Service%' then 'Self Service'
                when uro.name not like '%Self Service%' then 'MRA'
            end as owner

        from prod_ent_DB.salesforce.opportunity as o

        left join prod_ent_DB.salesforce.account as a

            on o.accountid = a.id

        left join
            community_db.ds_dsc.vertical_classification_sdr_accounts as il

            on a.id = il.sf_account_id

        left join prod_ent_DB.salesforce.user as uo

            on o.ownerid = uo.id

        left join prod_ent_DB.salesforce.userrole as uro

            on uo.userroleid = uro.id

        left join prod_ent_DB.salesforce.user as us

            on o.sourced_by__c = us.id

        left join prod_ent_db.salesforce.lead as l

            on o.contactid = l.convertedcontactid

        left join prod_ent_db.salesforce.contact as c

            on o.contactid = c.id


        left join (select
            fiscal_year_quarter,
            rq.month_name,
            rq.date,
            day_of_week_name,
            fiscal_week_num,
            day_of_fiscal_quarter_num,
            fiscal_quarter,
            rq.iso_year_month,
            fiscal_year,
            first_value(rq.date)
                over (partition by iso_running_week order by rq.date asc)
                as week_of_qtr

        from community_db.ent_static.reference_dates as rq) as rq

            on rq.date = to_date(date_interested__c)


        where
            io_year in ('2022','2023', '2024', '2025')
            and o.partner_type__c is null
            and o.id is not null
    )
    left join community_db.ent_static.inbound_intl_ios as i
        on i.opportunity_id = ios

    where
        (opp_source__c in ('Inbound', 'Other') or opp_source__c is null)
        and Inbound_Exclusion_Flag in ('Include')
        and io_date is not null
        and
        (
            (core_product__c = 'International' and i.opportunity_id is not null)
            or
            (core_product__c != 'International')
        )
    )

    select * from IOs;
    """
    
    # Execute the query and load results into a DataFrame
    df = pd.read_sql(io_query, conn)

    # Close the connection
    conn.close()
    
    return df



   