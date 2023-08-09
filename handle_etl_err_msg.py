import sys, os, json, re
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__))))

from utils import get_item_record, sfn_exec_info, my_exception, remove_non_ascii
import inspect
import pandas as pd 
import datetime
from pandas import json_normalize
import boto3


def etl_err_lack_of_glue_job(data_df):
    # -- handle missing glue job --#
    filter_missing_glue_job_df = data_df[data_df.glue_job.isnull()]

    final_job = []
    if len(filter_missing_glue_job_df) == 0:
        print("No failed job")
        merge_df = pd.DataFrame()
        return merge_df
    elif len(filter_missing_glue_job_df) != 0:
        print(filter_missing_glue_job_df.columns.values.tolist())
        for item in filter_missing_glue_job_df.values.tolist():
            job_id = item[0]
            sfn_arn = item[-2]
            print(sfn_arn)
            sfn_response_arn = sfn_exec_info(exec_arn=sfn_arn, sfn_type='list_executions')
            tmp_final_job = parse_value_sfn_list_executions(sfn_response_arn=sfn_response_arn, job_id=job_id.replace("job#", ""))
            final_job.append(tmp_final_job)
        
    if len(final_job) != 0:
        final_df = prepare_data_before_put_metric(final_job=final_job, data_df=data_df, err_type='missing_glue_jobs')
        return final_df

def prepare_data_before_put_metric(final_job,data_df,err_type):
    # -- convert list to pandas --#
    convert_to_list = []
    for item in final_job:
        if len(item) == 0:
            continue
        elif isinstance(item[0], dict):
            convert_to_list.append(item[0])
        else:
            convert_to_list.append(item[0][0])
    
    tmp_data_df = pd.json_normalize(convert_to_list)

    if err_type == 'missing_glue_jobs':
        tmp_data_df = tmp_data_df.sort_values(by=['job_id', 'exec_start_time'])
        merge_df = tmp_data_df.merge(data_df, on='job_id')
        adding_cols = ['glue_job_id', 'glue_job']
    elif err_type == 'glue_jobs':
        merge_df = tmp_data_df.merge(data_df, on='job_id')
        merge_df = merge_df.drop_duplicates(subset=["job_id", "data_zone", "job_consumer", "exec_start_time"])
        adding_cols = ['exec_name']
    
    for col in adding_cols:
        merge_df[col] = '-'
    
    # -- reorder columns --#
    merge_df = merge_df[['job_id', 'data_zone', 'job_consumer', 'step_function_name', 'exec_start_time', 'exec_name','err_msg','status', 'glue_job_id', 'glue_job']]

    return merge_df
    
     
def parse_value_sfn_list_executions(sfn_response_arn, job_id):
    print("parse_value_sfn_list_executions")
    string_today = datetime.datetime.now().date()
    final_job = []

    for idx, items in enumerate(sfn_response_arn['executions'],0):
        exec_start_date = items['startDate'].date()
        
        if string_today == exec_start_date:
            
            exec_arn = items['executionArn']
            double_check_job_name = exec_arn.split(":")[-1]
            
            if double_check_job_name.startswith(job_id):
                tmp_final_job = handle_etl_error_msg_list_executions(exec_arn=exec_arn, job_id=job_id.replace("job#", ""))
                if len(tmp_final_job) != 0:
                    final_job.append(tmp_final_job)
            else: continue
    return final_job
            
def handle_etl_error_msg_list_executions(exec_arn,job_id):
    print("handle_etl_error_msg_list_executions")
    client = boto3.client('stepfunctions')
    response = client.get_execution_history(
        executionArn=exec_arn
    )
    failed_state_info =[]
    
    for item in response['events']:
        exec_type = item['type']
        
        if exec_type == 'ExecutionFailed' and len(item['executionFailedEventDetails']) != 0:
            tmp_failed_state_info = parse_err_msg_fail_state_entered(item=item, err_type = exec_type)
            if len(tmp_failed_state_info) != 0:
                tmp_failed_state_info.update({
                    "step_function_name": exec_arn.split(":")[-2],
                    "exec_name": exec_arn.split(":")[-1],
                    "job_id": "job#" + job_id
                })
                failed_state_info.append(tmp_failed_state_info)
        elif exec_type == 'FailStateEntered':
            sfn_arn_failed_state = json.loads(item['stateEnteredEventDetails']['input'])['AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID']
            
            tmp_failed_state_info = parse_err_msg_fail_state_entered(item=item, err_type=exec_type)
            
            if len(tmp_failed_state_info) != 0:
                print(f"job_id: {job_id}")
                tmp_failed_state_info.update({
                    "step_function_name": sfn_arn_failed_state.split(":")[-2],
                    "exec_name": sfn_arn_failed_state.split(":")[-1],
                    "job_id": "job#" + job_id
                })
                failed_state_info.append(tmp_failed_state_info)
                

    return failed_state_info

def parse_err_msg_fail_state_entered(item, err_type):
    # -- parse value in the step function state --#
    exec_start_time = item['timestamp'].strftime("%Y-%m-%d %H:%M:%S")

    if err_type == 'ExecutionFailed':
        error_type = item['executionFailedEventDetails']['error']
        if error_type in['States.Runtime', 'Lambda.AWSLambdaException']:
            exec_cause = item['executionFailedEventDetails']['cause']
            err_msg = remove_non_ascii(a_str=exec_cause)
            failed_job_info = {
                "exec_start_time": exec_start_time, 
                "err_msg": err_msg
            }
        else:
            failed_job_info = []
    elif err_type == 'FailStateEntered':
        state_name = item['stateEnteredEventDetails']['name']
        
        state_entered_event_details = item['stateEnteredEventDetails']['input']

        # job_id = json.loads(state_entered_event_details)['job_id']
        glue_crwl_state =  json.loads(state_entered_event_details)['glue_crwl_state']
        
        if state_name == 'Fail state' and len(glue_crwl_state) != 0:
            err_msg = "Failed state at crawler step"
            job_status = glue_crwl_state['Payload']['status']

            if job_status == 'FAILED':
                failed_job_info = {
                    "exec_start_time": exec_start_time, 
                    "err_msg": err_msg,
                    "glue_job_id": "-"
                }
            else:
                failed_job_info = []
        
    return failed_job_info

def etl_err_has_glue_job(data_df):
    print("function etl_err_has_glue_job")
    filter_failed_job = data_df[data_df.glue_job.notnull()]

    
    if len(filter_failed_job) == 0:
        print("No failed job ")
        merge_df = pd.DataFrame()
        return merge_df
    elif len(filter_failed_job) != 0:
        err_job_list = []
        # -- check err msg glue job run --#
        glue_client = boto3.client("glue")
        
            
        for item in filter_failed_job.values.tolist():
            
            job_id = item[0]
            step_function_name = item[-2]
            glue_job_name = item[-1]
            print(f"*--*-- job_id: {job_id} --*--*")
            
            response = glue_client.get_job_runs(
                    JobName= glue_job_name, 
                    MaxResults=20
            )

            tmp_err_job = handle_response_glue_job_runs(response=response, job_id=job_id, step_function_name=step_function_name)
            
            if len(tmp_err_job) != 0:
                err_job_list.append(tmp_err_job)
            elif len(err_job_list) == 0:
                print("debug step function failed")
                # -- glue job executed successfull but full flow failed.
                sfn_arn = item[-2]
                
                sfn_response_arn = sfn_exec_info(exec_arn=sfn_arn, sfn_type='list_executions')
                tmp_final_job = parse_value_sfn_list_executions(sfn_response_arn=sfn_response_arn, job_id=job_id.replace("job#", ""))
                err_job_list.append(tmp_final_job)

        if len(err_job_list) != 0:
            final_df = prepare_data_before_put_metric(final_job=err_job_list, data_df=data_df, err_type='glue_jobs')
            
        return final_df

def handle_response_glue_job_runs(response, job_id,step_function_name):
    print("handle_response_glue_job_runs")
    err_msg_list = []
    today = datetime.datetime.now()
    for item in response['JobRuns']:
        start_time = item['StartedOn']
        str_start_time = start_time.strftime("%m/%d/%Y, %H:%M:%S")
        job_status = item['JobRunState']

        if today.date() == start_time.date() and job_status == 'FAILED':
            glue_job_id = item['Id']
            error_message = remove_non_ascii(a_str=item['ErrorMessage'])
            job_status = item['JobRunState']
            tmp_job_err = {
                "err_msg": error_message,
                "job_status": job_status,
                "glue_job_id": glue_job_id,
                "exec_start_time": str_start_time,
                "job_id": job_id,
                "step_function_name": step_function_name
                
            }
            err_msg_list.append(tmp_job_err)
        else:
            continue
    return err_msg_list