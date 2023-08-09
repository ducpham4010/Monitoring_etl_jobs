import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__))))

from utils import my_exception, get_item_record, process_dataframe
import inspect
import pandas as pd
from pandas import json_normalize

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
        raise my_exception(exception=e, exec_function_name=exec_function_name)