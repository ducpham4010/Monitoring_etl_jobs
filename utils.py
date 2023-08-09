import boto3
import logging
import pandas as pd
from pandas import json_normalize
import json
import datetime
import traceback, os, sys, inspect
import numpy as np
import time, re
import string
from functools import reduce
import urllib3
# turn off these warnings
pd.options.mode.chained_assignment = None

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

def my_exception(exception,exec_function_name):
    exc_type = sys.exc_info()
    
    msg = f"""
    ===================================================================================
        Unhandled exception: {exc_type} 
        in function {exec_function_name}
        err: {str(exception)}
        , {traceback.print_exc()}
    ===================================================================================
    """
    print(msg)
    sys.exit(1)

def convert_timestamp(date_time : str, format_ts: str):
    print (f"Start the function convert_human_timestamp ")
    if format_ts is None:
        format_ts = "%Y-%m-%d %H:%M:%S"
        if date_time is None:
            date_time = datetime.now().strftime(format_ts, )

        string_today = datetime.datetime.now().date().strftime("%d/%m/%Y") + " 00:00:00"
        start_time = datetime.datetime.strptime(string_today,"%d/%m/%Y %H:%M:%S")
        # -- convert to ms --#
        start_time_in_sc = int(datetime.datetime.timestamp(start_time)) * 1000
    else:
        start_time = datetime.datetime.strptime(start_time,"%d/%m/%Y %H:%M:%S")
        # # -- convert to ms --#
        start_time_in_sc = int(datetime.datetime.timestamp(start_time)) * 1000

    return start_time_in_sc
   
def get_item_record(qry_statement, nested_key_info):
    client = boto3.resource('dynamodb')
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
        raise  my_exception(exception=e, exec_function_name=exec_function_name)
    return list_job_name

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
        raise  my_exception(exception=e, exec_function_name=exec_function_name)
    return data_df
    
def fitler_data_by_job_frequency(data_df, config):
    try:
        print("Start the function fitler_data_by_job_frequency")
        # print(f"Total rows in dataframe: {len(data_df)}")
        
        skipped_status = config['skipped_status']
        
        df1 = data_df.loc[data_df['status'] != 'NOT_START']
        df1 = df1[~df1['job_frequency'].isin(skipped_status)]
        
        df2 = data_df.loc[data_df['status'] == 'NOT_START']
        # -- remove job_frequency in list skipped_status --#
        df2 = df2[~df2['job_frequency'].isin(skipped_status)]
        # -- filter and remove specific daily --#
        tmp_daily = df2[(df2['job_frequency'] == 'DAILY') & (df2['date_execute'].notnull()) ]        
        
        df2 = delete_row(filtered_df=tmp_daily, data_df=df2, condition_date_type='DAILY')
        
        tmp_monthly = df2[(df2['job_frequency'] == 'MONTHLY') & (df2['date_execute'].notnull()) ]
        
        df2 = delete_row(filtered_df=tmp_monthly, data_df=df2, condition_date_type='MONTHLY')
        
        tmp_end_of_month = df2[(df2['job_frequency'] == 'END_OF_MONTH') & (df2['date_execute'].notnull()) ]
        
        df2 = delete_row(filtered_df=tmp_end_of_month, data_df=df2, condition_date_type='END_OF_MONTH')
        
        tmp_weekly = df2[(df2['job_frequency'] == 'WEEKLY') & (df2['date_execute'].notnull()) ]
        
        df2 = delete_row(filtered_df=tmp_weekly, data_df=df2, condition_date_type='WEEKLY')
        
    except Exception as e:
        exec_function_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_function_name)
    else:
        frames = [df1, df2]
        data_df = pd.concat(frames)

    return data_df

def checking_condition_date(data_df, job_id):
    data_df = data_df[data_df['job_id'].str.contains(job_id)==False]

    return data_df

def checking_condition_month(today_month, data_df, today_day, job_id):
    if today_month == 2:
        if today_day != 28:
            data_df = data_df[data_df['job_id'].str.contains(job_id)==False]
    else:
        if today_day != 29:
            data_df = data_df[data_df['job_id'].str.contains(job_id)==False]
    return data_df
      
def delete_row(filtered_df, data_df, condition_date_type):
    # print("Start function delete_row")

    # -- checking monthly condition --#
    today_month = datetime.datetime.now().date().month
    today_day =  datetime.datetime.now().date().day 
    
    #weekday convert as int (1 == Sunday,2 == Monday,3 == Tuesday,4 == Wednesday,5 == Thursday,6 == Friday,7 == Saturday)
    today_weekday = datetime.date.today().toordinal()%7 + 1 
    try:
        for item in filtered_df.values.tolist():
            
            job_id = item[0]
            schedule = item[5].replace('"', '').split(',')

            try:
                schedule = list(map(int, schedule))
            except Exception as e:
                schedule = []
            # print(f"Job ID: {job_id} - schedule: {schedule}")
            
            if condition_date_type in ['Daily', 'DAILY', 'WEEKLY', 'MONTHLY'] and today_day not in schedule:
                data_df = checking_condition_date(data_df=data_df, job_id=job_id)
            
            elif condition_date_type == 'END_OF_MONTH':
                data_df = checking_condition_month(today_month=today_month, data_df=data_df, today_day=today_day, job_id=job_id)
            
            elif condition_date_type == 'WEEKLY' and today_weekday not in schedule:
                data_df = checking_condition_date(data_df=data_df, job_id=job_id)
    except Exception as e:
        exec_function_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_function_name)
    
    return data_df

def percentage_by_status(data_df, list_status):
    try:
        print("Starting the function: percentage_by_status")
        total_jobs = len(data_df)
        
        percentage_status = []
        
        percentage_status.append({
            "SCHEDULED": [total_jobs, "100%"]
        })
        
        for item in list_status:
            total = (data_df['status'] == item).sum()
            percent_val = round(((data_df['status'] == item).sum()/total_jobs) * 100, 2)            
            percentage_status.append({
                item: [total, f"{percent_val}%"]
            })
    except Exception as e:
        exec_function_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_function_name)
    
    return percentage_status

def percentage_by_type(data_df, list_job_type):
    print("Function percentage_by_type")
    
    data_df['counter'] = 1
    data_df['percent_status'] = 0
    
    try:
        totals = data_df.groupby(list_job_type)['counter'].sum()
        # -- convert series to dataframe --#
        percentages = totals.to_frame().reset_index()
        percentages['rate'] = round(100* percentages['counter']/percentages.groupby(list_job_type[:-1])['counter'].transform('sum'), 2)
        # -- add column schedule to dataframe -- do not use --#
        percentages['scheduled'] = percentages.groupby(list_job_type[:-1])['counter'].transform('sum')
        # -- add row schedule (total jobs) to data frame --#
        if 'job_consumer' in list_job_type:
            df2 = percentages.drop_duplicates(subset=["job_consumer", "data_zone", "scheduled"])
            df2 = df2[["job_consumer",'data_zone','scheduled']]
        elif 'job_source' in list_job_type:
            df2 = percentages.drop_duplicates(subset=["job_source", "scheduled"])
            df2 = df2[['job_source','scheduled']]
        else: 
            df2 = percentages.drop_duplicates(subset=["data_zone", "scheduled"])
            df2 = df2[['data_zone','scheduled']]

        df2['status'] = "SCHEDULED"
        df2["counter"] = df2['scheduled']
        df2['rate']  = 100
        
        # -- checking missing status 
        stt_df = add_missing_status(data_df=percentages, list_job_type=list_job_type)
        # -- reorder columns 
        if 'job_consumer' in list_job_type:
            df2 = df2[["job_consumer",'data_zone', 'status', 'counter', 'rate', 'scheduled']]
        elif 'job_source' in list_job_type:
            df2 = df2[["job_source", "status", "counter", 'rate', 'scheduled']]
        else:
            df2 = df2[['data_zone', 'status', 'counter', 'rate', 'scheduled']]
        # -- concat and reset index
        frames = [percentages, df2, stt_df]
        percentages = pd.concat(frames)
        # -- convert dataframe to json type
        percentages = percentages.where(pd.notnull(percentages), '-')
        percentages = percentages.reset_index(drop=True)

    except Exception as e:
        exec_function_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_function_name)

    else:
        # -- convert dataframe to json type
        json_list = json.loads(json.dumps(list(percentages.T.to_dict().values())))
    return json_list

def add_missing_status(data_df, list_job_type):
    try:
        print("Function add_missing_status")
        df = pd.DataFrame()
        
        # -- checking missing status group by job consumer or job stage --#
        check_list_status = ["FAILED", "NOT_START", "RUNNING", "SUCCEEDED", "SKIPPED"]
        
        if 'job_consumer' in list_job_type:
            missing_status_df = data_df.groupby(['job_consumer', 'data_zone'])['status'].unique()
        elif 'job_source' in list_job_type:
            missing_status_df = data_df.groupby(['job_source'])['status'].unique()
        else:
            missing_status_df = data_df.groupby(['data_zone'])['status'].unique()
        
        # -- convert series to dataframe and reset index--#
        tmp_df = missing_status_df.to_frame().reset_index()
        
        for item in tmp_df.values.tolist():
            if 'job_consumer' in list_job_type:
                # print(item)
                job_consumer = item[0]
                data_zone = item[1]
                stt_lst = item[2]
            elif 'job_source' in list_job_type:
                job_source = item[0]
                stt_lst = item[1]
            else:
                data_zone = item[0]
                stt_lst = item[1]
                
            # -- convert 'numpy.ndarray' to list and check missing status
            missing_status = [i for i in check_list_status if i not in stt_lst.tolist()]
            missing_stt_df = pd.DataFrame(missing_status, columns=['status'])
            
            if 'job_consumer' in list_job_type:
                missing_stt_df['job_consumer'] = job_consumer
                missing_stt_df['data_zone'] = data_zone
            elif 'job_source' in list_job_type:
                missing_stt_df['job_source'] = job_source
            else:
                missing_stt_df['data_zone'] = data_zone
            # -- adding some columns to the check missing df
            missing_stt_df['counter'] = 0
            missing_stt_df['rate'] = 0
            missing_stt_df['scheduled'] = 0
            # -- concat 2 dataframes
            frames = [df, missing_stt_df]
            df = pd.concat(frames,  axis=0)
            
            df = reorder_cols(df=df,list_job_type=list_job_type)
        return df
    except Exception as e:
        exec_function_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_function_name)


def reorder_cols(df,list_job_type):
    if 'job_consumer' in list_job_type:
        df = df[["job_consumer", 'data_zone', 'status', 'counter', 'rate', 'scheduled']]
    elif 'job_source' in list_job_type:
        df = df[["job_source", 'status', 'counter', 'rate', 'scheduled']]
    else:
        df = df[['data_zone', 'status', 'counter', 'rate', 'scheduled']]
    df = df.reset_index(drop=True)
    
    return df

def sfn_exec_info(exec_arn, sfn_type):
    sfn_client = boto3.client('stepfunctions')
    if sfn_type == 'list_executions':
        response = sfn_client.list_executions(
            stateMachineArn= exec_arn,
            maxResults=200,
            statusFilter='FAILED'
        )
    elif sfn_type == 'get_execution_history':
        response = sfn_client.get_execution_history(
            executionArn= exec_arn,
            maxResults = 20,
            reverseOrder = True
        )
    return response

def remove_non_ascii(a_str):
    list_special_chars = ['\\n', '\n', '\t', '\\', "\'"]
    
    for char in list_special_chars:
        a_str = a_str.replace(char, '')
    
    ascii_chars = set(string.printable)
    
    return ''.join(filter(lambda x: x in ascii_chars, a_str))

def unique_status(data_df):
    list_status = data_df['status'].values.tolist()
    unique_list_status = reduce(lambda l, x: l.append(x) or l if x not in l else l, list_status, [])
    
    return unique_list_status

def filter_data_by_status(status, data_df,format_date):
    format_columns = ['start_time', 'end_time']
    filtered_df = data_df[data_df['status'] == status]
    timezone_info = 'Asia/Saigon'
    if status == 'RUNNING':
        filtered_df = filtered_df[filtered_df['end_time'].isnull()]
        if len(filtered_df) != 0:
            filtered_df['duration'] = pd.Timestamp.utcnow().tz_convert(timezone_info) - pd.to_datetime(filtered_df['start_time'])
        else:                
            filtered_df['duration'] = pd.to_datetime(filtered_df['end_time']) - pd.to_datetime(filtered_df['start_time'])
    elif status == 'NOT_START':
        filtered_df['duration'] = 0
        # print(filtered_df['duration'])
    else:
        filtered_df['duration'] = pd.to_datetime(filtered_df['end_time']) - pd.to_datetime(filtered_df['start_time'])

    # if status != 'NOT_START':
    for col in format_columns:
        filtered_df[col] = pd.to_datetime(filtered_df[col], format=format_date)
        filtered_df[col] = filtered_df[col].dt.strftime(format_date)
     
    return filtered_df