import traceback, os, sys, inspect
import boto3
import json
import pandas as pd
import numpy as np
from pandas import json_normalize
import datetime

def parse_items(items):
    list_item = []
    for item in items:
        tmp_dict = {}
        for key, value in item.items():
            for k, v in value.items():
                if v is True:
                    v = None
                tmp_dict.update({key:v})
        list_item.append(tmp_dict)

    return list_item

def parse_data_in_json(items, nested_key_dict):
    list_item = []
    if 'pk' in nested_key_dict.values():
        # -- parsing data after fectching data on job_entry table --#
        key_dict_1 = nested_key_dict['key_1']
        key_dict_2 = nested_key_dict['key_2']
        replace_str = nested_key_dict['replace_str']
        for item in items:    
            tmp_job_name = item.get(key_dict_1).get(key_dict_2).replace(replace_str, '') # -- strim and get job name --#
            list_item.append(tmp_job_name)
    else:
        list_item = parse_items(items=items)

    return list_item

def get_item_record(qry_statement, nested_key_info):
    client = boto3.client('dynamodb')
    try:
        # print(qry_statement)
        print("qry_statement", qry_statement)
        response = client.execute_statement(
            Statement= qry_statement
        )
        items = response["Items"]
        print("list_job_name_V1")
        # -- parse job name in list items --#
        list_job_name = parse_data_in_json(items=items, nested_key_dict=nested_key_info)
        print("list_job_name", list_job_name)
        # print("list_job_name", list_job_name)
        # print("items", items)
        # print("response", response)
        while "LastEvaluatedKey" in response or "NextToken" in response:
            # -- fetch data in while loop --#
            response = client.execute_statement(
                Statement=qry_statement, NextToken = response["NextToken"]
            )
            if len(response["Items"]) != 0:
                tmp_list_job_name = parse_data_in_json(items=response["Items"], nested_key_dict=nested_key_info)
                list_job_name.extend(tmp_list_job_name)
    except Exception as e:
        exec_function_name = inspect.currentframe().f_code.co_name
        raise e
    return list_job_name

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

def get_context_info (context):
    print("Starting the function parsing_context_lambda_info")
    
    try:
        lambda_arn = context.invoked_function_arn
        lambda_request_id = context.aws_request_id
        lambda_memory_limit_in_mb = context.memory_limit_in_mb
        cw_stream_name = context.log_stream_name
        cw_loggroup_name = context.log_group_name
    except Exception:
        # -- for unittest --#
        lambda_arn = context['invoked_function_arn']
        lambda_request_id = context['aws_request_id']
        lambda_memory_limit_in_mb = context['memory_limit_in_mb']
        cw_stream_name = context['log_stream_name']
        cw_loggroup_name = context['log_group_name']
    
    context_info = {
        "LambdaARN": lambda_arn,
        "LambdaRequestId": lambda_request_id,
        "LambdaMemoryLimitInMb": lambda_memory_limit_in_mb,
        "log_stream_name": cw_stream_name, 
        "log_group_name": cw_loggroup_name, 
    }
    # print(context_info)
    return context_info

def convert_human_timestamp(start_time):
    print (f"Start the function convert_human_timestamp ")
    
    if start_time is None:
        string_today = datetime.datetime.now().date().strftime("%d/%m/%Y") + " 00:00:00"
        start_time = datetime.datetime.strptime(string_today,"%d/%m/%Y %H:%M:%S")
        # -- convert to ms --#
        start_time_in_sc = int(datetime.datetime.timestamp(start_time)) * 1000
    else:
        start_time = datetime.datetime.strptime(start_time,"%d/%m/%Y %H:%M:%S")
        # # -- convert to ms --#
        start_time_in_sc = int(datetime.datetime.timestamp(start_time)) * 1000

    return start_time_in_sc

def process_dataframe(data_df, cols): 
    try:
        # -- convert start time and end time to human timestamp 
        list_cols = ['start_time', 'end_time']
        for col in list_cols:
            try:
                data_df[col] = pd.to_datetime(data_df[col], unit='ms', utc=True).dt.tz_convert('Asia/Saigon')
            except Exception as e:
                continue
        
        data_df['duration'] = data_df['end_time'] - data_df['start_time']
        
        data_df['duration'] = data_df['duration'].astype(object)
        data_df = data_df.groupby('job_id', as_index=False).last()
        
        condition = [
            data_df['start_time'].isnull()
        ]
        choicelist = ['NOT_START']
        data_df['status'] = np.select(condition, choicelist, data_df['status'])
        
        # -- reorder columns --#
        data_df = data_df[cols]
    except Exception as e:
        exec_function_name = inspect.currentframe().f_code.co_name
        raise  e
    return data_df


def etl_normal_jobs(extra_condition, config, env, list_jobs):
    try:
        """ Loading configure varibales from json file """
        step = config['step'] # "step": 50
        
        reorder_cols = config['reorder_cols'] # "reorder_cols": ["job_id", "data_zone", "job_consumer", "job_stage", "job_frequency", "date_execute", "sla_starttime", "status", "start_time", "end_time", "duration", "sla_endtime"]
        data_df = pd.DataFrame()
        
        # -- metadata table information --#
        metadata_tbl_name = config[env]['dynamodb_info_job_metadata']['table_name']       # "table_name": "tcb-ode-sit-job-metadata_v4"
        metadata_cols_name = config[env]['dynamodb_info_job_metadata']['cols']            # "cols": ["job_id", "job_consumer", "job_stage", "data_zone", "job_frequency", "sla_starttime", "date_execute", "sla_endtime"]
        
        nested_key_dict_metadata = {
                "key_1": "start_time", 
                "list_key": metadata_cols_name,
                "replace_str": config[env]['dynamodb_info_job_metadata']['key_type_string']  # "key_type_string": "S"
        }
        tmp_qry_statement_get_metadata = config[env]['dynamodb_info_job_metadata']['qry']   #"qry": "SELECT %(cols_name)s FROM \"%(table_name)s\" WHERE  begins_with(\"%(col_name)s\", '%(condition)s') and is_disable = false  and tags[0] = 'curated' "
        
        # -- job log table information --#
        job_log_tbl_name = config[env]['dynamodb_info_job_log']['table_name']
        job_log_cols_name = config[env]['dynamodb_info_job_log']['cols']

        nested_key_dict_job_log = {
            "key_1": "job_stage", 
                "list_key": job_log_cols_name, # "cols": ["job_id", "start_time", "end_time", "status"],
                "replace_str": config[env]['dynamodb_info_job_log']['key_type_string'] # "key_type_string": "S"
        }
        tmp_qry_statement_job_log = config[env]['dynamodb_info_job_log']['qry'] # "qry": "SELECT %(cols)s FROM \"%(table_name)s\" WHERE job_id in (%(jobs)s)"
        # -- ########## variables ########## -- #
        
        # -- ########## BEGIN THE SCRIPT ########## -- #
        print(f"Total jobs table: {len(list_jobs)}")
        
        for i in range(0, len(list_jobs), step):
            next_step = i + step
            # print(f"next step: {next_step}")
            str_job_name = ','.join(f"'{x}'" for x in list_jobs[i:next_step])

            qry_statement_get_metadata = tmp_qry_statement_get_metadata % {
                "table_name": metadata_tbl_name,
                "jobs": str_job_name,
                "cols": ','.join(metadata_cols_name)
            }
            
            
            list_metadata_job = get_item_record(qry_statement= qry_statement_get_metadata, nested_key_info=nested_key_dict_metadata)
            # -- convert json data to dataframe --#
            tmp_metadata_job_df = json_normalize(list_metadata_job)
            # -- drop duplicated --#
            tmp_metadata_job_df = tmp_metadata_job_df.drop_duplicates()
    
            
            # print(f"Total rows of metadata: {len(tmp_metadata_job_df)}")
            
            if len(tmp_metadata_job_df) != 0:
                # -- get joblog -- #
                tmp_metadata_jobs = tmp_metadata_job_df['job_id'].values.tolist()
                
                for i in range(0, len(tmp_metadata_jobs), step):
                    next_step = i + step
                    # print(f"i: {i}; step: {next_step}")
                    str_tmp_metadata_jobs = ','.join(f"'{x}'" for x in tmp_metadata_jobs)
                    qry_statement_get_job_log = tmp_qry_statement_job_log % {
                        "table_name": job_log_tbl_name,
                        "jobs":str_tmp_metadata_jobs,
                        "cols": ','.join(job_log_cols_name),
                        "extra_condition" : extra_condition
                    }
                    print("qry_statement_get_job_log", qry_statement_get_job_log)
                    print("nested_key_dict_job_log", )
                    tmp_list_job_log = get_item_record(qry_statement= qry_statement_get_job_log, nested_key_info=nested_key_dict_job_log)
                    print("tmp_list_job_log", tmp_list_job_log)
                    if len(tmp_list_job_log) != 0:
                        # -- convert json to dataframe -- 
                        tmp_job_log_df = json_normalize(tmp_list_job_log)
                        
                        # -- drop duplicated rows
                        tmp_job_log_df = tmp_job_log_df.sort_values(by=['job_id', 'end_time'])
                        tmp_job_log_df = tmp_job_log_df.drop_duplicates(keep='last')
                        
                        # -- merge data frame
                        tmp_data_df = pd.merge(tmp_job_log_df, tmp_metadata_job_df, on=['job_id'], how='outer')
                        tmp_data_df = process_dataframe(data_df=tmp_data_df, cols=reorder_cols)
                        
                        # -- append current dataframe to existed dataframe
                        frames = [data_df,tmp_data_df]
                        data_df = pd.concat(frames)

        # --writing data to csv file --#
        print(f"Total rows after processing data: {len(data_df)}")
        return data_df
        # -- ########## END THE SCRIPT ########## -- #
    except Exception as e:
        exec_function_name = inspect.currentframe().f_code.co_name
        raise e

def lambda_handler(event, context):
    if context is not None:
        lambda_loggroup = get_context_info(context=context)
        configuration_file = open('config.json')
    env = event['env']
    daily =  event['daily']
    config = json.load(configuration_file)
    list_jobs = get_list_jobs_name(config=config, env=env)
    print("list job : ", list_jobs)
    print("NORMAL JOB :")
    etl_jobs = list_jobs[0]
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
    etl_jobs_df = etl_normal_jobs(extra_condition=extra_condition, config=config, env=env, list_jobs=etl_jobs)
    print("###############################")
    print(etl_jobs_df)