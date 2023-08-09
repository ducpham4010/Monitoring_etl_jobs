import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__))))

import inspect, datetime, time, json
from put_metric import put_metric_status_only, put_metric_data, put_metric_data_failed_jobs
from put_metric import put_err_msg_to_cloudwatch_cds, put_metric_data_not_start_jobs, put_metric_job_duration
from put_metric import put_metric_duration_stage
from put_metric import put_err_msg_to_cloudwatch_etl
from statictics import report_statictics
# from etl_oncloud_sourcing import etl_oncloud_sourcing
from get_jobs_name import etl_get_job_information
from utils import get_context_info, convert_human_timestamp, my_exception
from handle_etl_err_msg import etl_err_lack_of_glue_job, etl_err_has_glue_job

def process_etl_jobs(etl_jobs_df,config, namespace, env):
    report_etl_job_stage = report_statictics(config=config, report_type = 'job_stage', data_df=etl_jobs_df, metric_type=None)    
    report_etl_job_consumer = report_statictics(config=config, report_type = 'job_consumer', data_df=etl_jobs_df, metric_type=None)
    
    # -- stage's duration
    put_metric_duration_stage(data_df=etl_jobs_df, namespace=namespace, metric_type='job_stage')
    put_metric_data(json_list=report_etl_job_stage, metric_type='job_stage', namespace=namespace)
    # put_metric_data(json_list=report_etl_job_consumer, metric_type='job_consumer', namespace=namespace)
    # print("put_metric_data_job_consumer", put_metric_data)
    put_metric_data_failed_jobs(data_df=etl_jobs_df, namespace=namespace, metric_type='job_stage')
    # put_metric_data_failed_jobs(data_df=etl_jobs_df, namespace=namespace, metric_type='job_consumer')
    # print("put_metric_data_failed_jobs_job_consumer", put_metric_data_failed_jobs)
    # -- put metrics  and not start job to cloudwatch --#
    # put_metric_data_not_start_jobs(data_df=etl_jobs_df, namespace=namespace, metric_type='job_consumer')
    # print("put_metric_data_not_start_jobs", put_metric_data_not_start_jobs)
    # -- putmetric: duration job 
    # put_metric_job_duration(data_df=etl_jobs_df, namespace=namespace, metric_type='job_consumer')
    # print("put_metric_job_duration", put_metric_job_duration)
    
    # -- get and put err msg from glue job to cloudwatch
    etl_job_info_df = etl_get_job_information(data_df=etl_jobs_df, config=config, env=env, report_type=None)

    # -- detail's err job --#
    if len(etl_job_info_df) != 0:
        filter_missing_glue_job_df = etl_err_lack_of_glue_job(data_df=etl_job_info_df)
        filter_failed_job_included_glue_job = etl_err_has_glue_job(data_df=etl_job_info_df)
        
        if len(filter_missing_glue_job_df) != 0:
            put_err_msg_to_cloudwatch_etl(data_df=filter_missing_glue_job_df, namespace=namespace)
        
        if len(filter_failed_job_included_glue_job) != 0:
            put_err_msg_to_cloudwatch_etl(data_df=filter_failed_job_included_glue_job, namespace=namespace)
            
    return True