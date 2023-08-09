import sys, os, json, re
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__))))

from utils import get_item_record, sfn_exec_info, my_exception, remove_non_ascii
import inspect
import pandas as pd 
import datetime
from pandas import json_normalize
import boto3

def cds_get_job_err(data_df):
    print("Start the function: cds_get_job_err")
    final_job = []
    
    string_today = datetime.datetime.now().date()
    sfn_arn = data_df.task_execution_step_fn.unique()
    
    sfn_response_arn = sfn_exec_info(exec_arn=sfn_arn[0], sfn_type='list_executions')
    for idx, items in enumerate(sfn_response_arn['executions'],0):
        exec_start_date = items['startDate'].date()
        if string_today == exec_start_date:
            exec_arn = items['executionArn']            
            tmp_final_job = handle_error_msg_list_executions(exec_arn=exec_arn)
            final_job.append(tmp_final_job)
            
    # -- convert list to pandas --#
    if len(final_job) != 0:
        convert_to_list = []
        for item in final_job:
            if len(item) == 0:
                continue
            elif isinstance(item[0], dict):
                convert_to_list.append(item[0])
            else:
                convert_to_list.append(item[0][0])
        tmp_data_df = pd.json_normalize(convert_to_list)
        
        filtered_data_df = data_df[['job_id', 'job_source']]
        merge_df = tmp_data_df.merge(filtered_data_df, on='job_id')
        
        # -- drop duplicated rows group by columns --#
        merge_df = merge_df.drop_duplicates(subset=["job_id", "job_source", "exec_start_time"])
        
        # -- reorder columns --#
        merge_df = merge_df[['job_id','err_msg', 'step_function_name', 'glue_job_id', 'exec_start_time', 'job_source', 'job_status', 'glue_job_name', 'exec_name']]
        
    else:
        merge_df = pd.DataFrame()
    return merge_df
    
def handle_error_msg_list_executions(exec_arn):
    client = boto3.client('stepfunctions')
    response = client.get_execution_history(
        executionArn=exec_arn
    )
    failed_state_info =[]
    for item in response['events']:
        
        exec_type = item['type']
        
        if exec_type == 'TaskFailed' :
            tmp_failed_state_info = parse_err_msg_fail_state_entered(item)
            if len(tmp_failed_state_info) != 0:
                tmp_failed_state_info.update({
                    "step_function_name": exec_arn.split(":")[-2],
                    "exec_name": exec_arn.split(":")[-1]
                })
                failed_state_info.append(tmp_failed_state_info)
    
    return failed_state_info
            
def parse_err_msg_fail_state_entered(item):
    # -- parse value in the step function state --#
    exec_start_time = item['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        error_type = item['error']
    except KeyError:
        error_type = item['taskFailedEventDetails']['error']
    
    if error_type == 'States.TaskFailed':
        exec_cause = json.loads(item['taskFailedEventDetails']['cause'])
        job_name = exec_cause['Arguments']['--job_entry_id']
        try:
            err_msg = exec_cause['ErrorMessage']
        except KeyError:
            err_msg = "No err msg found in the failed state"
        glue_job_name = exec_cause['JobName']
        glue_job_id = exec_cause['Id']
        job_status = exec_cause['JobRunState']

        err_msg = remove_non_ascii(a_str=err_msg)
        failed_job_info = {
            "exec_start_time": exec_start_time, 
            "err_msg": err_msg,
            "glue_job_name": glue_job_name,
            "glue_job_id": glue_job_id,
            "job_status": job_status,
            "job_id": "job#" + job_name
        }
    else:
        failed_job_info = []
    return failed_job_info