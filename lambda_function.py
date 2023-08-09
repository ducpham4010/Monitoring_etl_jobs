import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__))))

import inspect, datetime, time, json
from etl_normal_jobs import etl_normal_jobs
from statictics import report_statictics
from get_jobs_name import get_list_jobs_name
from utils import get_context_info, convert_human_timestamp, my_exception
from process_etl_jobs import process_etl_jobs

def lambda_handler(event, context):
    try:
        print("*--*-- Starting the script --*--*")
        start_time_script = datetime.datetime.now()
        start = time.time()
        
        print(f"Start the script: {start_time_script}")
        print(event)
        if context is not None:
            lambda_loggroup = get_context_info(context=context)
            configuration_file = open('config.json')
        else:
            lambda_loggroup = 'Running at local'
            configuration_file = open('config.json')
        # -- loading event from lambda --#
        
        env = event['env']
        daily =  event['daily']

        # -- environment -- #
        msg = f"The environment variable is: {env}"
        print(msg)
                
        config = json.load(configuration_file)
        namespace = config[env]['CloudwatchNameSpace']
        if daily == 1:
            # convert timestamp human to int (ms)
            # converted_time = convert_human_timestamp(start_time=None)
            converted_time = '2023-03-15 00:00:00'
            extra_condition = f"and start_time >= {converted_time}"
        else:
            if 'start_time' in event:
                start_time = event['start_time']
                end_time = event['end_time']
            else:
                start_time = '2023-03-15 00:00:00'
                end_time = '2023-03-16 00:00:00'
            
            converted_start_time = convert_human_timestamp(start_time=start_time)
            converted_end_time = convert_human_timestamp(start_time=end_time)
            
            extra_condition = f"and start_time BETWEEN {converted_start_time} and {converted_end_time}"
        
        
        print(extra_condition)

        # -- start the function -- #
        list_jobs = get_list_jobs_name(config=config, env=env) #  ex : [['rollup_daily_curated_t24_cst_to_idv', 'fss_orcs_glue_job']]
        print("Get list job name done")
        etl_jobs = list_jobs[0]
        print("Get list job name done V2")
        # oncloud_sourcing_jobs = list_jobs[1]
        
        print("*-*-*-*-*-*-*-*-*-*- RAW - Oncloud Sourcing -*-*-*-*-*-*-*-*-*-*")
        # oncloud_jobs_df = etl_oncloud_sourcing(list_job_name=oncloud_sourcing_jobs,extra_condition = extra_condition, config= config, env=env)
        
        # if len(oncloud_jobs_df) != 0:
            # process_cds_jobs(oncloud_jobs_df=oncloud_jobs_df, config=config, namespace=namespace, env=env)
        
        # -- etl jobs --#
        print("*-*-*-*-*-*-*-*-*-*- Standard - Curated - Product and Insight Zone -*-*-*-*-*-*-*-*-*-*")
        etl_jobs_df = etl_normal_jobs(extra_condition=extra_condition, config=config, env=env, list_jobs=etl_jobs)         
        if len(etl_jobs_df) != 0:
            # -- staticstic --#
            report_etl_status_only = report_statictics(config=config, report_type = 'status_only', data_df=etl_jobs_df,metric_type=None)
            # # filter_etl_job_df is a dataframe after applied the date execution
            filter_etl_job_df = report_etl_status_only[0]
            process_etl_jobs(etl_jobs_df=etl_jobs_df,config=config, namespace=namespace, env=env)
        
        end_time_script = datetime.datetime.now()
        end = time.time()
        total_time = end - start
        print(f"--------- Total seconds to execute the script: {str(total_time)} --------- ")
        
        print(f"End the script: {end_time_script}")

        final_message = {
            "StartTime": str(start_time_script),
            "EndTime": str(end_time_script),
            "ExecDuration": str(total_time),
            "loggroup": lambda_loggroup
        }
        print(final_message)
        return final_message
          
    except Exception as e:
        func_name_err = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=func_name_err)
        
    
if __name__ == '__main__':
    event = {
        "env": "sit", 
        "daily": 1
    }
    print(event)
    
    lambda_handler(n=event, context=None)