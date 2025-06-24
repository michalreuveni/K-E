import pandas as pd
from snowflake_access.SQLUtils import query_on_multiple_cids


query = """
select
    SM_VERSION_CLIENT,
    count(*) as scores,
    min(starttime) as min_starttime,
    max(starttime) as max_starttime,
    100 * div0null(count_if(IS_RAT and score_action = 'getscore'), count_if(score_action = 'getscore')) as rat_pct_getscore,
    100 * div0null(count_if(IS_RAT and score_action = 'getscorelogin'), count_if(score_action = 'getscorelogin')) as rat_pct_login,
    100 * div0null(count_if(IS_RAT and score_action = 'terminate_session'), count_if(score_action = 'terminate_session')) as rat_pct_terminatation,
    100 * div0null(count_if(FC_RAT_RESAMPLING_SCORE_WITH_CONTEXT >= 200 and score_action = 'getscore'), count_if(score_action = 'getscore')) as rat2_pct_getscore,
    100 * div0null(count_if(FC_RAT_RESAMPLING_SCORE_WITH_CONTEXT >= 200 and score_action = 'getscorelogin'), count_if(score_action = 'getscorelogin')) as rat2_pct_login,
    100 * div0null(count_if(FC_RAT_RESAMPLING_SCORE_WITH_CONTEXT >= 200 and score_action = 'terminate_session'), count_if(score_action = 'terminate_session')) as rat2_pct_termination
from sessions_snapshots_v2
inner join (
    select sid, SM_VERSION_CLIENT
    from sessions
    where starttime > current_date - 3
    and session_type = 'normal'
    and SM_VERSION_CLIENT > '2.44') using (sid)
where starttime > current_date - 3
and product = 'ATO'
and session_type = 'normal'
and score_action in ('getscore', 'getscorelogin', 'terminate_session')
and sm_device_source = 'js'
and sm_platform_type = 1
group by 1
order by 1
"""

# Execute the query on multiple CIDs
query_df = query_on_multiple_cids(query, [])

query_df.to_csv('/Users/michal.reuveni/Desktop/Pycharm results/Lior.csv')

print(f"Results saved")






