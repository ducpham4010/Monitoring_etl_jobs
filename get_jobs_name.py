import sys, os, json, re
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__))))

from utils import get_item_record, sfn_exec_info, my_exception
import inspect
import pandas as pd 
import datetime
from pandas import json_normalize

def get_list_jobs_name(config, env):
    etl_list_job = []
    cols_name = "pk, tags"
    nested_key_dict_jobs = {
            "key_1": config[env]['dynamodb_info_job_entry']['primary_key'],
            "key_2": config[env]['dynamodb_info_job_entry']['key_type_string'],
            "replace_str": config[env]['dynamodb_info_job_entry']['prefix_job_name']
    }
    
    qry_statement_get_all_jobs_name = config[env]['dynamodb_info_job_entry']['qry'] %{
        "table_name": config[env]['dynamodb_info_job_entry']['table_name'],
        "cols_name": cols_name,
        "condition": nested_key_dict_jobs['replace_str'],
        "col_name": nested_key_dict_jobs['key_1']
    }
    
    list_jobs = get_item_record(qry_statement=qry_statement_get_all_jobs_name, nested_key_info=nested_key_dict_jobs)
    
    for item in list_jobs:
        etl_list_job.append(item)
    
    return [etl_list_job]

def etl_get_job_information(data_df, config, env, report_type):
    
    print("Start function etl_get_job_information")
    
    job_entry_cols = "pk, step_function, step_function_input['glue_job']"
        
    job_entry_tbl_name = config[env]['dynamodb_info_job_entry']['table_name']
    
    failed_df = data_df[data_df['status']=='FAILED']
    
    if len(failed_df) == 0:
        data_df = []
        print("No failed job....")
    else:
        data_df = pd.DataFrame() # empty dataframe
        step = config['step']
        failed_df['job_id'] = failed_df['job_id'].apply(lambda x: f"job#{x}")
        list_failed_job = failed_df['job_id'].values.tolist()
        
        for i in range(0, len(list_failed_job), step):
            next_step = i + step
            
            string_failed_job = ','.join(f"'{x}'" for x in list_failed_job[i:next_step])
            
            qry = """SELECT %(cols_name)s FROM "%(table_name)s" WHERE pk in (%(job_name)s) """ % {
                "cols_name": job_entry_cols,
                "table_name": job_entry_tbl_name,
                "job_name": string_failed_job
            }

            list_jobs = get_item_record(qry_statement=qry, nested_key_info={})
            
            # -- convert json to df, rename column, and merge df
            jobs_df = json_normalize(list_jobs)
            print("Count df", jobs_df.count())
            jobs_df = jobs_df.rename(columns={"pk":"job_id"})
            
            # tmp_data_df = failed_df.merge(jobs_df, on='job_id')
            # print("Count df tmp", tmp_data_df.count())

            frames = [data_df]
            data_df = pd.concat(frames)
            data_df = data_df.drop_duplicates()
    
    return data_df

def sfn_parse_err_execution(item, exec_arn, job_name):
    
    final_job = []
    sfn_arn = exec_arn.split(":")[-2]
    exec_start_time = item['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        tmp_err_msg = item['executionFailedEventDetails']['error']
        err_cause = item['executionFailedEventDetails']['cause']
    except KeyError:
        tmp_err_msg = "Null as err msg in SFN"
        err_cause = "Null as cause in SFN"

    err_msg = tmp_err_msg + " " + "cause: " + err_cause
    job_name = "_".join(job_name.split("_")[:-2])
    
    tmp_final_job = {
            "job_id": f"job#{job_name}",
            "error_message": err_msg,
            "step_function_name": sfn_arn,
            "task_id": "",
            "exec_startTime": exec_start_time
        }
    

    final_job.append(tmp_final_job)
    
    return final_job

def handle_error_msg_list_executions(response,string_today):
    final_job = []
    for idx, items in enumerate(response['executions'],0):
        exec_start_date = items['startDate'].date()
        job_name = items['name']
        
        
        if string_today == exec_start_date:
            exec_arn = items['executionArn']
            response_exec_history = sfn_exec_info(exec_arn=exec_arn, sfn_type='get_execution_history')
            
            tmp_final_job = handle_error_msg_get_execution_history(response_exec_history, exec_arn=exec_arn, job_name=job_name)
            final_job.append(tmp_final_job)
        else: continue
    print("function handle_error_msg_list_executions")
    print(f"Total len: {len(final_job)}")
    return final_job
    
def handle_error_msg_get_execution_history(response_exec_history,exec_arn, job_name):
    final_job = []
    for item in response_exec_history['events']:
        exec_type = item['type']

        if exec_type == 'FailStateEntered' and item['stateEnteredEventDetails']['name'] == 'Fail state':            
            state_info = json.loads(item['stateEnteredEventDetails']['input'])
            
            job_id = state_info['job_id']

            if 'task_list' in state_info:
                if 'task_list' in state_info['prepare_job_output']['Payload']:
                    task_exec_sfn = state_info['config']['config']['task_execution_step_fn']
                    
                    tmp_task_list_id = state_info['prepare_job_output']['Payload']['task_list']
                    task_list_id = re.sub('[^a-zA-Z0-9 \n\.]', '', tmp_task_list_id).split()

                    sub_task_response = sfn_exec_info(exec_arn=task_exec_sfn, sfn_type='list_executions')
                    
                    # print(f"JobName: {job_id} - Sub task list id: {task_list_id}")
                    final_job = handle_error_msg_in_glue_job(task_list_id=task_list_id, sub_task_response=sub_task_response, job_id=job_id, item=item)
                else:
                    sfn_arn = exec_arn.split(":")[-2]
                    exec_start_time = item['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
                    # normal case
                    subtask_state_entered_event_deatils_err = json.loads(item['stateEnteredEventDetails']['input'])['error']['Cause']
                    err_msg = json.loads(subtask_state_entered_event_deatils_err)['errorMessage']
                    tmp_final_job = {
                        "job_id": f"job#{job_id}",
                        "error_message": err_msg,
                        "step_function_name": sfn_arn,
                        "task_id": "",
                        "exec_startTime": exec_start_time
                    }
                    print(f"handle_error_msg_get_execution_history - err msg job: {tmp_final_job}")

                final_job.append(tmp_final_job)
            else: continue
        elif exec_type == 'ExecutionFailed':
            final_job = sfn_parse_err_execution(item=item, exec_arn=exec_arn, job_name=job_name)
    return final_job

def sfn_parse_err_execution(item, exec_arn, job_name):
    print("function sfn_parse_err_execution")
    final_job = []
    sfn_arn = exec_arn.split(":")[-2]
    exec_start_time = item['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        tmp_err_msg = item['executionFailedEventDetails']['error']
        err_cause = item['executionFailedEventDetails']['cause']
    except KeyError:
        tmp_err_msg = "Null as err msg in SFN"
        err_cause = "Null as cause in SFN"

    err_msg = tmp_err_msg + " " + "cause: " + err_cause
    job_name = "_".join(job_name.split("_")[:-2])
    print(job_name)
    tmp_final_job = {
            "job_id": f"job#{job_name}",
            "error_message": err_msg,
            "step_function_name": sfn_arn,
            "task_id": "",
            "exec_startTime": exec_start_time
        }
    print(f"sfn_parse_err_execution - err msg job: {tmp_final_job}")

    final_job.append(tmp_final_job)
    
    return final_job

def handle_error_msg_in_glue_job(task_list_id ,sub_task_response, job_id, item):
    final_job = []
    for task_id in task_list_id:
        tmp_sub_task_id = job_id + '_' + task_id
        
        for err_exec in sub_task_response['executions']:
            err_arn = err_exec['executionArn']
            err_exec_name = "_".join(err_exec['name'].split("_")[:-1])
            if tmp_sub_task_id.endswith(err_exec_name):
                subtask_exec_history = sfn_exec_info(exec_arn=err_arn, sfn_type='get_execution_history')
                
                tmp_final_job = handle_error_msg_json(subtask_exec_history=subtask_exec_history, err_arn=err_arn, item=item, job_id=job_id, task_id=task_id)
                final_job.append(tmp_final_job)
    return final_job

def handle_error_msg_json(subtask_exec_history, err_arn, item, job_id, task_id):
    final_job = []
    for err_subtak in subtask_exec_history['events']:
        exec_type = err_subtak['type']
        sfn_arn = str(err_arn.split(":")[-2])
        exec_start_time = err_subtak['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        
        if exec_type == 'FailStateEntered' and item['stateEnteredEventDetails']['name'] == 'Fail state':
            subtask_state_entered_event_deatils_err = json.loads(err_subtak['stateEnteredEventDetails']['input'])['glue_job_output']['Cause']
            subtask_err_msg = json.loads(subtask_state_entered_event_deatils_err)['ErrorMessage']

            tmp_final_job = {
                    "job_id": f"job#{job_id}",
                    "error_message": subtask_err_msg,
                    "step_function_name": sfn_arn,
                    "task_id": task_id,
                    "exec_startTime": exec_start_time
            }
            final_job.append(tmp_final_job)
    return final_job