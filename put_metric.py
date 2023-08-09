import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__))))

import boto3 
import inspect, json
from functools import reduce
import pandas as pd
from pandas import json_normalize
from utils import my_exception, remove_non_ascii, unique_status, filter_data_by_status, sfn_exec_info
import datetime
from handle_etl_err_msg import etl_err_lack_of_glue_job, etl_err_has_glue_job

standard_format_date = "%Y-%m-%d"
format_date = standard_format_date + " %H:%M:%S.%f%z"
format_date_no_milsec = standard_format_date + " %H:%m:%s"
timezone_info = "Asia/Saigon"

def put_metric_status_only(json_list, namespace):
    try:
        print("Start function : put_metric_status_only ")
        print(f"Name space: {namespace}")
        print(f"Total items: {len(json_list)}")
        client = boto3.client('cloudwatch')
        count = 0
        for item in json_list:
            for key, value in item.items():
                status = key
                counter = int(value[0])
                dimension = [
                        {
                            "Name": "Status", 
                            "Value": status
                        }
                ]

                metric_data = [{
                    'MetricName': f"Number of job {status}",
                    "Dimensions": dimension,
                    'Value': counter,
                    'Unit': "Count"
                }]
                
                response = client.put_metric_data(
                        Namespace= namespace,
                        MetricData= metric_data
                )
                count += 1
        print(f"Total item put to CW: {count}")
    except Exception as e:
        exec_func_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_func_name)

    return response
    
def put_metric_data(json_list, namespace, metric_type):
    try:
        print(f"Start function : put_metric_data - {metric_type}")
        print(f"Total items: {len(json_list)}")
        client = boto3.client('cloudwatch')
        count = 0
        for item in json_list:
            if metric_type == 'job_stage':
                dimension = [
                            {
                                "Name": "JobStage",
                                "Value": item['data_zone']
                            }
                    ]
            elif metric_type == 'job_consumer':
                job_consumer = item['job_consumer']
                
                if job_consumer == '':
                    job_consumer = '-'
                
                dimension = [
                        {
                            "Name": "JobStage", 
                            "Value": item['data_zone']
                        },
                        {
                            "Name": "JobConsumer",
                            "Value": job_consumer
                        }
                ]
            elif metric_type == 'job_test':
                dimension = [
                        {
                            "Name": "JobSource", 
                            "Value": item['job_source']
                        }
                ]
            
            metric_data = [{
                'MetricName': f"Number of job {item['status']}",
                "Dimensions": dimension,
                'Value': int(item['counter']),
                'Unit': "Count"
            }]
            
            # print(f"Metric data: {metric_data}")
            response = client.put_metric_data(
                Namespace= namespace,
                MetricData= metric_data
            )
            count += 1
        print(f"Total item put to CW: {count}")
    except Exception as e:
        print(metric_data)
        exec_func_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_func_name)
    return response

def put_metric_data_failed_jobs(data_df, namespace, metric_type):
    
    print(f"Start function put_metric_data_failed_jobs - {metric_type}")
    print(f"Total rows: {len(data_df)}")
    
    failed_jobs_df = data_df[data_df['status'] == 'FAILED']
    
    if len(failed_jobs_df) == 0:
        msg = "No job fail today"
        return msg
    elif len(failed_jobs_df) != 0:
        # -- replace nan to NONE
        if {'duration'}.issubset(failed_jobs_df.columns):
            failed_jobs_df = failed_jobs_df.drop(columns=['duration'])
        
        failed_jobs_df = failed_jobs_df.where(pd.notnull(failed_jobs_df), '-')

        cols_list = ['start_time', 'end_time']
        for col in cols_list:
            failed_jobs_df[col] = pd.to_datetime(failed_jobs_df[col], format=format_date)
            failed_jobs_df[col] = failed_jobs_df[col].dt.strftime(format_date)
        
        json_list = json.loads(failed_jobs_df.assign(**failed_jobs_df.select_dtypes(['datetime']).astype(str).to_dict('list') ).to_json(orient="records"))
        
        response = put_metric_failed_job(json_list=json_list, metric_type=metric_type, namespace=namespace)
        
        return response

def put_metric_failed_job(json_list, metric_type, namespace):
    print("Start the function put_metric_failed_job")
    print(f"total item in json_list: {len(json_list)}")
    response = []
    count = 0 
    try:
        client = boto3.client('cloudwatch')
        print(f"Name space: {namespace}")
        for item in json_list:
            if metric_type == 'job_stage':
                dimension = [
                    {
                        "Name": "JobStage", 
                        "Value": item['data_zone']
                    },
                    {
                        "Name": "JobName",
                        "Value": item['job_id']
                    }
                    ,{
                        "Name": "StartTime",
                        "Value": item['start_time']
                    }
                    ]
            elif metric_type == 'job_consumer':
                job_consumer = item['job_consumer']
                if job_consumer == '':
                    job_consumer = '-'
                dimension = [
                    {
                        "Name": "JobStage", 
                        "Value": item['data_zone']
                    },
                    {
                        "Name": "JobConsumer",
                        "Value": job_consumer
                    },
                    {
                        "Name": "JobName",
                        "Value": item['job_id']
                    }
                    ,{
                        "Name": "StartTime",
                        "Value": item['start_time']
                    }]
            elif metric_type == 'oncloud_sourcing':
                dimension = [
                        {
                            "Name": "JobSource", 
                            "Value": item['job_source']
                        },
                        {
                            "Name": "JobName",
                            "Value": item['job_id']
                        },
                        {
                            "Name": "StartTime",
                            "Value": item['start_time']
                        }
                    ]      
            metric_data = [{
                'MetricName': f"Number of job {item['status']}",
                "Dimensions": dimension,
                'Value': 1,
                'Unit': "Count"
            }]
            tmp_response = client.put_metric_data(
                Namespace= namespace,
                MetricData= metric_data)
            response.append(tmp_response)
            count += 1
        print(f"Total item put to CW: {count}")
    except Exception as e:
        exec_func_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_func_name)
    return response

def put_err_msg_to_cloudwatch_etl(data_df, namespace):
    print("Start function put_err_msg_to_cloudwatch_etl")
    cloudwatch_client = boto3.client("cloudwatch")
    print(f"Total etl err jobs: {len(data_df)}")
    data_df = data_df.where(pd.notnull(data_df), '-')
    
    print(data_df.columns.values.tolist()) 

    count = 0
    
    for item in data_df.values.tolist():
        job_id = item[0].split('job#')[-1]
        job_stage = item[1]
        job_consumer = item[2]
        step_function_name = item[3]
        str_start_time = item[4]
        exec_name = item[5]
        error_message = item[6]
        job_status = item[7]
        glue_job_id = item[8]
        glue_job_name = item[9]
        
        if ":" in step_function_name:
            step_function_name = step_function_name.split(":")[-1]
        
        dimension = [
            {
                "Name": "JobStage",
                "Value": job_stage
            },
            {
                "Name": "JobConsumer",
                "Value": job_consumer
            },
            {
                "Name": "ErrMsg",
                "Value": error_message
            },
            {
                "Name": "JobName",
                "Value": job_id
            },
            {
                "Name": "StartTime",
                "Value": str_start_time
            },
            {
                "Name": "GlueJobName",
                "Value": glue_job_name
            },
            {
                "Name": "StepFunctioName",
                "Value": step_function_name
            },
            {
                "Name": "ExecName",
                "Value": exec_name
            },
            {
                "Name": "GlueJobId",
                "Value": glue_job_id
            }
            ]
                
        metric_data = [{
            'MetricName': f"{job_status}",
            "Dimensions": dimension,
            'Value': 1,
            'Unit': "Count"
        }]

        try:
            response = cloudwatch_client.put_metric_data(
                    Namespace= namespace,
                    MetricData= metric_data
            )
            count += 1
        except Exception as e :
            print("Error.... could not put metric to cloudwatch")
            print(metric_data)
            print(e)
    print(f"Total metric put to CW: {count}")
    return response

def put_err_msg_to_cloudwatch_cds(data_df, namespace):
    print("Start the function put_err_msg_to_cloudwatch_cds")
    print(f"Total rows: {len(data_df)}")
    cloudwatch_client = boto3.client("cloudwatch")    
    data_df = data_df.where(pd.notnull(data_df), '-')
    count = 0 
        
    for item in data_df.values.tolist():
        job_name = item[0].split('job#')[-1]
        error_message = remove_non_ascii(a_str=item[1])
            
        step_function_name = item[2]
        task_id = item[3]
        if task_id == "":
            task_id = '-'
        exec_start_time = item[4]
        job_source = item[5]
        job_status = item[6]
        glue_job_name = item[7]
        exec_name = item[8]
        
        dimension = [
            {
                "Name": "JobSource",
                "Value": job_source
            },
            {
                "Name": "JobName",
                "Value": job_name
            },
            {
                "Name": "TaskId",
                "Value": task_id
            },
            {
                "Name": "ErrMsg",
                "Value": error_message
            },
            {
                "Name": "ExecStartTime",
                "Value": exec_start_time
            },
            {
                "Name": "StepFunctionName",
                "Value": step_function_name
            },
            {
                "Name": "ExecutionName",
                "Value": exec_name
            },
            {
                "Name": "GlueJobName",
                "Value": glue_job_name
            }
            
        ]
        
        metric_data = [{
            'MetricName': f"{job_status}",
            "Dimensions": dimension,
            'Value': 1,
            'Unit': "Count"
        }] 
        
        try:
            response = cloudwatch_client.put_metric_data(
                    Namespace= namespace,
                    MetricData= metric_data
            )
            count += 1
        except Exception:
            print("Error.... could not put metric to cloudwatch")
            print(metric_data)
    print(f"Total metric put to CW: {count}")
    return response

def put_metric_data_not_start_jobs(data_df, namespace, metric_type):
    try:
        print(f"Start function put_metric_data_not_start_jobs - {metric_type}")
        count = 0 
        format_columns = ['start_time', 'end_time']
        # -- convert TimeStamp to string
        for col in format_columns:
            data_df[col] = pd.to_datetime(data_df[col], format=format_date)
            data_df[col] = data_df[col].dt.strftime(format_date)
        
        not_start_jobs_df = data_df[data_df['status'] == 'NOT_START']

        if len(not_start_jobs_df) == 0:
            response = "No NOT_START jobs are pending"
            return response 
        elif len(not_start_jobs_df) != 0:
            # -- replace nan to NONE
            print(f"Total rows: {len(not_start_jobs_df)}")
            # -- replace NAN, NONE value to -
            not_start_jobs_df = not_start_jobs_df.where(pd.notnull(not_start_jobs_df), '-')
            # -- convert dataframe to json --#
            json_list = json.loads(not_start_jobs_df.assign(**not_start_jobs_df.select_dtypes(['datetime']).astype(str).to_dict('list') ).to_json(orient="records"))
            
            client = boto3.client('cloudwatch')
            
            # -- generating demension --#
            for item in json_list:
                if metric_type == 'job_consumer':
                    job_consumer = item['job_consumer']
                    if job_consumer == '':
                        job_consumer = '-'
                    dimension = [
                            {
                                "Name": "JobStage", 
                                "Value": item['data_zone']
                            },
                            {
                                "Name": "JobConsumer",
                                "Value": job_consumer
                            },
                            {
                                "Name": "JobName",
                                "Value": item['job_id']
                            },
                            {
                                "Name": "JobFrequency",
                                "Value": item['job_frequency']
                            }
                    ]
                elif metric_type == 'oncloud_sourcing':
                    dimension = [
                            {
                                "Name": "JobSource", 
                                "Value": item['job_source']
                            },
                            {
                                "Name": "JobName",
                                "Value": item['job_id']
                            }
                    ]
               
                metric_data = [{
                    'MetricName': f"Number of job {item['status']}",
                    "Dimensions": dimension,
                    'Value': 1,
                    'Unit': "Count"
                }]
                try:
                    response = client.put_metric_data(
                        Namespace= namespace,
                        MetricData= metric_data
                    )
                    count += 1
                except Exception as e:
                    print(e)
                    raise e
            
    except Exception as e:
        exec_func_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_func_name)
    print(f"Total metric put to CW: {count}")
    return response

def put_metric_job_duration(data_df, namespace, metric_type):
    try:
        client = boto3.client('cloudwatch')
        print(f"Start the function : put_metric_job_duration - {metric_type}")
        print(f"Total rows: {len(data_df)}")
        # -- get distinct status in the dataframe 
        count = 0 
        unique_list_status = unique_status(data_df=data_df)
        
        for status in unique_list_status:
            # print(status)
            filtered_df = filter_data_by_status(status, data_df,format_date)

            # -- get end_time values is null
            if status != 'NOT_START':
                filtered_df['duration'] = round((filtered_df['duration'] / pd.Timedelta(1, unit='m')),2)
            # -- convert TimeStamp to string -- 
            filtered_df = filtered_df.where(pd.notnull(filtered_df), '-')
            json_list = json.loads(filtered_df.assign(**filtered_df.select_dtypes(['datetime']).astype(str).to_dict('list') ).to_json(orient="records"))
            
            for item in json_list:
                
                if metric_type == 'job_consumer':
                    job_consumer = item['job_consumer']
                    
                    if job_consumer == '':
                        job_consumer = '-'
                        
                    dimension = [
                        {
                            "Name": "JobStage", 
                            "Value": item['data_zone']
                        },
                        {
                            "Name": "JobConsumer",
                            "Value": job_consumer
                        },
                        {
                            "Name": "JobName",
                            "Value": item['job_id']
                        },
                        {
                            "Name": "JobFrequency",
                            "Value": item['job_frequency']
                        },
                        {
                            "Name": "StartTime",
                            "Value": item['start_time']
                        },
                        {
                            "Name": "EndTime",
                            "Value": item['end_time']
                        },
                        {
                            "Name": "Status",
                            "Value": item['status']
                        }
                    ]
                elif metric_type == 'oncloud_sourcing':
                    dimension = [
                            {
                                "Name": "JobStage",
                                "Value": "CDS"
                            },
                            {
                                "Name": "JobSource", 
                                "Value": item['job_source']
                            },
                            {
                                "Name": "JobName",
                                "Value": item['job_id']
                            },
                            {
                                "Name": "StartTime",
                                "Value": item['start_time']
                            },
                            {
                                "Name": "EndTime",
                                "Value": item['end_time']
                            },
                            {
                                "Name": "Status",
                                "Value": item['status']
                            }
                    ]
            
                metric_data = [{
                    'MetricName': "JobDuration",
                    "Dimensions": dimension,
                    'Value': item['duration'],
                    'Unit': "Seconds"
                }]
                response = client.put_metric_data(
                    Namespace= namespace,
                    MetricData= metric_data
                )
                count += 1
    
    except Exception as e:
        print("Error during put metric")
        print(metric_data)
        exec_func_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_func_name)
    print(f"Total metric put to CW: {count}")
    return response

def put_metric_duration_stage(data_df, namespace, metric_type):
    try:
        client = boto3.client('cloudwatch')
        print(f"Start function put_metric_duration_stage - {metric_type} ")
        
        list_job_stage = data_df['data_zone'].values.tolist()
        unique_list_job_stage = reduce(lambda l, x: l.append(x) or l if x not in l else l, list_job_stage, [])

        for job_stage in unique_list_job_stage:
            # print(job_stage)
            # -- filter data based on job_stage 
            tmp_df_get_duration = data_df[data_df['data_zone'] == job_stage]

            tmp_df_get_duration['start_time'] = tmp_df_get_duration['start_time'].fillna(pd.Timestamp.utcnow().tz_convert(timezone_info))
            
            tmp_df_get_duration['end_time'] = tmp_df_get_duration['end_time'].fillna(pd.Timestamp.utcnow().tz_convert(timezone_info))
            
            # -- get min start_date, max end_date --#
            min_start_date = tmp_df_get_duration['start_time'].min()
            max_end_date = tmp_df_get_duration['end_time'].max()
            
            # -- convert pandas timestamp to string
            str_min_start_date = min_start_date.to_pydatetime().strftime("%Y-%m-%d %H:%m:%s")
            str_max_end_date = max_end_date.to_pydatetime().strftime("%Y-%m-%d %H:%m:%s")
            
            duration = max_end_date - min_start_date
            total_duration = round((duration / pd.Timedelta('1 hour')),2)
            
            if total_duration < 0:
                total_duration = 0
            
            dimension = [
                {
                    "Name": "JobStage", 
                    "Value": job_stage
                },
                {
                    "Name": "StartTime",
                    "Value": str_min_start_date
                },
                {
                    "Name": "EndTime",
                    "Value": str_max_end_date
                }
            ]
    
            metric_data = [{
                'MetricName': f"Duration",
                "Dimensions": dimension,
                'Value': total_duration,
                'Unit': "None"
            }]
            
            response = client.put_metric_data(
                Namespace= namespace,
                MetricData= metric_data
            )
        
    except Exception as e:
        exec_func_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_func_name)
    return response

def put_metric_sla_stage(data_df, namespace):
    try:
        client = boto3.client('cloudwatch')
        sla_time = ['sla_starttime', 'sla_endtime']
        list_job_stage = data_df['data_zone'].values.tolist()
        unique_list_job_stage = reduce(lambda l, x: l.append(x) or l if x not in l else l, list_job_stage, [])

        for job_stage in unique_list_job_stage:
            # print(job_stage)
            # -- filter data based on job_stage 
            tmp_df_get_duration = data_df[data_df['data_zone'] == job_stage]

            tmp_df_get_duration['start_time'] = tmp_df_get_duration['start_time'].fillna(pd.Timestamp.utcnow().tz_convert(timezone_info))
            
            tmp_df_get_duration['end_time'] = tmp_df_get_duration['end_time'].fillna(pd.Timestamp.utcnow().tz_convert(timezone_info))
            
            # -- get min start_date, max end_date --#
            min_start_date = tmp_df_get_duration['start_time'].min()
            max_end_date = tmp_df_get_duration['end_time'].max()
            
            # -- convert pandas timestamp to string
            str_min_start_date = min_start_date.to_pydatetime().strftime("%Y-%m-%d %H:%m:%s")
            str_max_end_date = max_end_date.to_pydatetime().strftime("%Y-%m-%d %H:%m:%s")
            
            duration = max_end_date - min_start_date
            total_duration = round((duration / pd.Timedelta('1 hour')),2)
            
            if total_duration < 0:
                total_duration = 0
            
            dimension = [
                {
                    "Name": "JobStage", 
                    "Value": job_stage
                },
                {
                    "Name": "StartTime",
                    "Value": str_min_start_date
                },
                {
                    "Name": "EndTime",
                    "Value": str_max_end_date
                }
            ]
    
            metric_data = [{
                'MetricName': f"Duration",
                "Dimensions": dimension,
                'Value': total_duration,
                'Unit': "None"
            }]
            
            response = client.put_metric_data(
                Namespace= namespace,
                MetricData= metric_data
            )
    
    except Exception as e:
        exec_func_name = inspect.currentframe().f_code.co_name
        raise  my_exception(exception=e, exec_function_name=exec_func_name)
    return response