import pandas as pd
from snowflake_access.SQLUtils import query_on_multiple_cids


query = """
select
    sm_platform_type,to_date(starttime) as date,
    ifnull(SM_SITE_SOURCE, 'none') as brand,
    SM_DEVICE_SOURCE,
    count(distinct sid) as total,
    100 * round(
        sum(
            case
                when FC_PCT_DUPLICATIONS_IN_KEY_EVENTS > 0 then 1
                else 0
            end
        ) / count(distinct sid),
        3
    ) as DUPL_KEY_EVENTS_per,
    100 * round(
        sum(
            case
                when FC_PCT_DUPLICATIONS_IN_KEY_EVENTS> 10 then 1
                else 0
            end
        ) / count(distinct sid),
        3
    ) as DUPL_KEY_EVENTS_10_per,
    case
        when DUPL_KEY_EVENTS_per > 0.5 then 'fail ' || round(DUPL_KEY_EVENTS_per / 0.5, 1) || ' x thresh'
        else 'pass'
    end as dulp_key_compliance,
    100 * round(
        sum(
            case
                when FC_PCT_DUPLICATIONS_IN_ELEMENT_EVENTS > 0 then 1
                else 0
            end
        ) / count(distinct sid),
        3
    ) as DUPL_ELEMENT_EVENTS_per,
    100 * round(
        sum(
            case
                when FC_PCT_DUPLICATIONS_IN_ELEMENT_EVENTS > 10 then 1
                else 0
            end
        ) / count(distinct sid),
        3
    ) as DUPL_ELEMENT_EVENTS_10_per,
    case
        when DUPL_ELEMENT_EVENTS_per > 0.5
        and sm_device_source <> 'android' then 'fail' || round(DUPL_ELEMENT_EVENTS_per/ 0.5, 1) || ' x thresh'
        else 'pass'
    end as dulp_compliance
from
    sessions_snapshots_v2
where
    to_date(starttime) >=current_date()-30
    and score_action = 'terminate_session'
    and FC_PCT_DUPLICATIONS_IN_KEY_EVENTS is not null
   
group by
    all



"""

# Execute the query on multiple CIDs
query_df = query_on_multiple_cids(query, ['basher'])

query_df.to_csv('/Users/michal.reuveni/Desktop/Pycharm results/dulicatrions.csv')

print(f"Results saved")






