#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ETL V2 CHANGE LOG
# 
# v2.0.0      8/09/21   reference version
# v2.0.1     16/09/21   add quote parameters to BQ load routine 
#
# v2.1.0     20/09/21   add date to request_id for staging tables
# 
# v2.2.0     22/09/21   fully decouple trigger and check, no Receive_Log
# v2.2.1     23/09/21   code simplification, updated method for config items
# v2.2.2     23/09/21   amend regex match
# 
# v2.3.0     15/10/21   add Active column in metadata
# v2.3.1     19/10/21   change Active switch: use config column
# v2.3.2     28/10/21   add log if step skipped
# 
# TODO:  develop solution for optional key values for file load config
# TODO:  p_return_message as json (email: Lei, 27/10 15:21 )
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import os
import re
import json
import time
from google.cloud import storage, bigquery
from datetime import datetime
from pytz import timezone
import pandas_gbq as pg
import uuid

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def define_log_structure():

    log_field_list = ['LOG_UUID', 
                      'JOB_INSTANCE', 
                      'LOG_TIMESTAMP', 
                      'REQUEST_LOG_EVENT', 
                      'REQUEST_ID', 
                      'JOB_NAME', 
                      'JOB_VERSION', 
                      'STEP', 
                      'STEP_NAME', 
                      'STATUS', 
                      'TYPE', 
                      'SUBTYPE', 
                      'CONTROL_FLAG', 
                      'LOAD_MODE', 
                      'CONFIGURATION', 
                      'SOURCE_TYPE', 
                      'SOURCE_NAME', 
                      'TARGET_TYPE', 
                      'TARGET_NAME', 
                      'LOG_MESSAGE', 
                      'LOG_USER']
    
    return dict.fromkeys(log_field_list,'')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def write_execution_log(log_columns):

    time.sleep(1)

    time_utc = ''
    time_local = ''
    time_utc, time_local = get_time_now()

    L = GD_LOG_DICT.copy()
    L.update(log_columns)

    L ['LOG_UUID'] = str(uuid.uuid4())
    L ['LOG_TIMESTAMP'] = time_utc

    insert_statement = get_sql_insert (L, GS_EXECUTION_LOG)
    if GB_VERBOSE:
        print('write execution log: ' + insert_statement)

    try:
        G_BQ_CLIENT.query(insert_statement)
    except Exception as e:
        print('error writing execution log')
        print(e)
        
    return None

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_local_time_now():

    now = datetime.now()
    local_now = now.astimezone(timezone('Australia/Sydney'))

    return local_now.strftime("%Y-%m-%dT%H:%M:%S")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_utc_time_now():

    now = datetime.now()
    return now.strftime("%Y-%m-%dT%H:%M:%S")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_time_now():

    now = datetime.now()
    local_now = now.astimezone(timezone('Australia/Sydney'))

    time_utc = now.strftime("%Y-%m-%dT%H:%M:%S")
    time_local = local_now.strftime("%Y-%m-%dT%H:%M:%S")

    return time_utc, time_local 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_json_item (J_str, item):

    if GB_VERBOSE:
        print ('get json str: ' + J_str)
        print ('get json item: ' + item)

    try:
        json_item = json.loads(J_str)[item]
    except:
        json_item = ''
        
    return json_item

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def active(config):
    
    answer = True
    
    if config == '':
        answer = True
    else:
        active_value = get_json_item(config.upper(),'ACTIVE')
        if active_value == 'N':
            answer = False

    return answer

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def match(v1, v2):

    # v1 = v1.upper().replace(' ','_')
    v1 = v1.upper()
    v2 = v2.upper().replace('*','\w*')

    if re.match(v2, v1):
        answer = True
    else:
        answer = False

    return answer

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_sql_insert(D, table_name):
    
    #   D: dictionary
    #   LD: list of D
    
    LD = list(D)
    sentence = 'insert %s (' % table_name + ','.join(LD) + ') values (' +\
               ','.join(['%({})r'.format(field) for field in LD]) + ');'
    return (sentence % D)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_all_metadata(table):

    ret_val = ''
    meta = []

    try:
        TABLE_REF = '`' + table + '`'
        query = 'SELECT * FROM ' + TABLE_REF + ' WHERE TRUE'
    
        meta = pg.read_gbq(query, 
                           project_id=GS_PROJECT_ID, 
                           progress_bar_type=None)
        meta.fillna('',inplace=True)
        meta.sort_values(by=['JOB_NAME','STEP'], inplace=True)

    except Exception as e:
        ret_val = e

    return ret_val, meta

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_job(meta_df, search_name):

    # replace spaces with underscores - allows the \w* regex to work
    # search_name = search_name.replace(' ','_')

    status_ok = True
    trigger_dict = {}
    job_meta_df = []

    # triggers_df = meta_df.loc[(meta_df.CONTROL_FLAG == 'TRIGGER') & 
    #                           (meta_df.ACTIVE.str.upper() != 'N')].copy()

    triggers_df = meta_df.loc[meta_df.CONTROL_FLAG == 'TRIGGER'].copy() 

    for i in range(len(triggers_df)):

        if match(search_name,triggers_df.iloc[i].SOURCE_NAME):

            if active(triggers_df.iloc[i].CONFIGURATION):
                trigger_dict = triggers_df.iloc[i].to_dict()
                job_name = triggers_df.iloc[i].JOB_NAME
                job_meta_df = meta_df.loc[meta_df.JOB_NAME==job_name].copy()
                job_meta_df.sort_values(by=['STEP'],inplace=True)

                if GB_VERBOSE:
                    print('get_job: match: ' + search_name)

    if len(trigger_dict) == 0 or len(job_meta_df) == 0:
        status_ok = False
        if GB_VERBOSE:
            print ('get_job: len(trigger_dict): ' + str(len(trigger_dict)))
            print ('get_job: len(job_meta_df): ' + str(len(job_meta_df)))
        
    return status_ok, trigger_dict, job_meta_df

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_blobs(source_name):

    bucket_name = source_name.split('/')[0]
    blob_search = source_name[len(bucket_name)+1:]

    bucket = G_STG_CLIENT.bucket(bucket_name)

    blob_list = []
    for this_blob in list(bucket.list_blobs()):
        if match(this_blob.name, blob_search):
            blob_list.append (this_blob)

    return blob_list

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def add_request_id(table_id, step_request_id) :

    write_status_ok = True
    message = ''
    try:
        job_text = 'UPDATE `' + table_id + '` ' \
                    + 'SET REQUEST_ID = "'+ step_request_id + '" ' \
                    + 'WHERE TRUE'
        job = G_BQ_CLIENT.query(job_text)
        result = job.result()  
    except Exception:
        write_status_ok = False  
        for e in result.errors:  
            message = e['message']  
    
    return write_status_ok, message

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def load_to_staging(source, cfg_str, target, step_request_id):

    required_config = ['allow_jagged_rows',
                        'skip_leading_rows',
                        'source_format',
                        'field_delimiter',
                        'quote',
                        'truncate_before_load']

    if cfg_str == '':
        write_status_ok = False
        message = 'load config in metadata is empty'
    else:
        cfg_dict = json.loads(cfg_str)
    
        # diff_count = len(list(required_config-cfg_dict.keys())) \
        #             + len(list(cfg_dict.keys()-required_config))

        # check that all required keys are here
        # "active":"N" will previously prevent this step from executing
        # (any config items other than required_config will be ignored)
        diff_count = len(list(required_config-cfg_dict.keys())) 
        
        if diff_count > 0:

            write_status_ok = False
            message = 'invalid load config'

            if GB_VERBOSE:
                print (list(required_config-cfg_dict.keys()))
                print (list(cfg_dict.keys()- required_config))

        else:

            if cfg_dict['truncate_before_load'].upper() == 'YES' :
                write_disp=bigquery.WriteDisposition.WRITE_TRUNCATE
            else:
                write_disp=bigquery.WriteDisposition.WRITE_APPEND

            job_cfg = bigquery.LoadJobConfig(
                        allow_jagged_rows = cfg_dict['allow_jagged_rows'],
                        skip_leading_rows = cfg_dict['skip_leading_rows'],
                        source_format = cfg_dict['source_format'],
                        field_delimiter = cfg_dict['field_delimiter'],
                        quote_character = cfg_dict['quote'],
                        allow_quoted_newlines = False,
                        write_disposition = write_disp)
    
            uri = 'gs://' + source
            table_id = GS_PROJECT_ID + '.' + target
    
            if GB_VERBOSE:
                print('load: uri: ' + uri)
                print('load: table_id: ' + table_id)
                # print('load: job_config: ' + job_cfg)

            try:
                load_job = G_BQ_CLIENT.load_table_from_uri(uri, 
                                                           table_id, 
                                                           job_config=job_cfg)
                load_job.result()
                
                add_request_id(table_id, step_request_id)
    
                write_status_ok = True
                message = ''

            except Exception:
                write_status_ok = False
                message = 'load failed'

    return write_status_ok, message

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def call_procedure(procedure, p_request_id, p_load_mode, p_load_config):

    load_status_ok = True
    return_message = ''
    return_df = []
    
    query = """
    declare p_return_message string;
    CALL PROC_NAME ('REQUEST_ID', 'LOAD_MODE', 'LOAD_CONFIG', p_return_message);
    select p_return_message;
    """
    query = query.replace('PROC_NAME', procedure)
    query = query.replace('REQUEST_ID', p_request_id)
    query = query.replace('LOAD_MODE', p_load_mode)
    query = query.replace('LOAD_CONFIG', p_load_config)

    try:
        return_df = G_BQ_CLIENT.query(query).to_dataframe().copy()
        return_message = return_df.iloc[0].p_return_message
        if return_message == '':
            load_status_ok = True
        else:
            load_status_ok = False
                   
    except Exception:
        load_status_ok = False

    if not load_status_ok:
        if return_message == '':
            return_message = 'procedure error'

    return load_status_ok, return_message 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def move_blob(source_bucket_name, source_blob_name, target_address):
    
    ret_val = ''
    
    source_file_name = source_blob_name.split('/')[-1]      # always gets the filename part
    target_address = target_address.replace('GS://','')     # always remove 'GS://'

    # target_address can include a "subfolder" name
    target_address_parts = target_address.split('/')
    target_bucket_name = target_address_parts[0]     # first part is always the bucket_name

    # if more than 1 part, it contains subfolder(s) - isolate the prefix for the new blob
    target_prefix = ''
    if len(target_address_parts) > 1 :
        target_prefix = target_address[len(target_bucket_name)+1:] + '/'

    target_blob_name = target_prefix + source_file_name

    try:
        source_bucket =  G_STG_CLIENT.get_bucket(source_bucket_name)
        target_bucket = G_STG_CLIENT.get_bucket(target_bucket_name)

        source_blob = source_bucket.blob(source_blob_name)

        source_bucket.copy_blob(source_blob, 
                                target_bucket, 
                                target_blob_name)
        source_blob.delete()

        move_status_ok = True

    except Exception as e:
        move_status_ok = False
        ret_val = e
        ret_val = 'error archiving blob'

    return move_status_ok, ret_val

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def delete_blob(blob):

    try:
        blob.delete()
        delete_status_ok = True

    except Exception as e:
        print('exception:',e)
        delete_status_ok = False
    
    return delete_status_ok

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def action_step(step, job_log):

    log = job_log.copy()
    status_ok = True

    # log column          where set
    # -----------------   ---------
    # LOG_UUID            write_log
    # JOB_INSTANCE        main
    # LOG_TIMESTAMP       write_log
    # REQUEST_LOG_EVENT   main, step
    # REQUEST_ID          main, step init (this section)
    # JOB_NAME            main
    # JOB_VERSION         main
    # STEP                step init
    # STEP_NAME           step init
    # STATUS              step
    # TYPE                step init
    # SUBTYPE             step init
    # CONTROL_FLAG        step init
    # LOAD_MODE           step init
    # CONFIGURATION       step init
    # SOURCE_TYPE         step init
    # SOURCE_NAME         step init, step
    # TARGET_TYPE         step init
    # TARGET_NAME         step init
    # LOG_MESSAGE         step init (clear), step
    # LOG_USER            (not used)

    log['REQUEST_ID'] = get_local_time_now()
    log['STEP'] = step.STEP
    log['STEP_NAME'] = step.STEP_NAME
    log['TYPE'] = step.TYPE
    log['SUBTYPE'] = step.SUBTYPE
    log['CONTROL_FLAG'] = step.CONTROL_FLAG
    log['LOAD_MODE'] = step.LOAD_MODE
    log['CONFIGURATION'] = step.CONFIGURATION
    log['SOURCE_TYPE'] = step.SOURCE_TYPE
    log['SOURCE_NAME'] = step.SOURCE_NAME
    log['TARGET_TYPE'] = step.TARGET_TYPE
    log['TARGET_NAME'] = step.TARGET_NAME
    log['LOG_MESSAGE'] = ''

    step_active = active(step.CONFIGURATION)
    log['LOG_MESSAGE'] = 'skipped'

    #-------------------------------------------------------------------------

    if step.CONTROL_FLAG == 'TRIGGER':
        # already handled in the main function
        step = step

    #-------------------------------------------------------------------------

    elif step.TYPE == 'CHECK':

        log['REQUEST_LOG_EVENT'] = GS_START
        log['STATUS'] = GS_INFO
        write_execution_log(log)

        reqd = get_json_item(step.CONFIGURATION,'file')
        if reqd == '':
            reqd = 1

        if GB_VERBOSE:
            print('reqd: ' + str(reqd))

        if reqd > 1:
            time.sleep(int(GN_SLEEP_SECS))

        blobs = get_blobs(step.SOURCE_NAME)

        if GB_VERBOSE:
            for i in range(len(blobs)):
                print(blobs[i].bucket.name + '/' + blobs[i].name)

        if len(blobs) == reqd:
            log['STATUS'] = GS_INFO
            log['LOG_MESSAGE'] = 'passed file count check'
        else:
            log['STATUS'] = GS_ERROR
            log['LOG_MESSAGE'] = 'failed file count check'
            status_ok = False

        log['REQUEST_LOG_EVENT'] = GS_END
        write_execution_log(log)

    #-------------------------------------------------------------------------

    elif step.TYPE == 'FILE' and step.SUBTYPE == 'LOAD':

        log['REQUEST_LOG_EVENT'] = GS_START
        log['STATUS'] = GS_INFO
        write_execution_log(log)

        load_status_ok = True
        message = ''
        load_status_ok, message = load_to_staging(step.SOURCE_NAME,
                                                  step.CONFIGURATION,
                                                  step.TARGET_NAME,
                                                  log['REQUEST_ID'])

        if load_status_ok :
            log['STATUS'] = GS_INFO
            log['LOG_MESSAGE'] = 'file load success'
        else:
            log['STATUS'] = GS_ERROR
            log['LOG_MESSAGE'] = message
            status_ok = False

        log['REQUEST_LOG_EVENT'] = GS_END
        write_execution_log (log)

    #-------------------------------------------------------------------------

    elif step.TYPE == 'FILE' and step.SUBTYPE == 'ARCHIVE':
        
        move_status_ok = True
        success_counter = 0

        blobs = get_blobs(step.SOURCE_NAME)
        bucket_name = blobs[0].bucket.name

        for i in range(len(blobs)):

            blob_name = blobs[i].name
            full_name = blobs[i].bucket.name + '/' + blobs[i].name
            log['REQUEST_LOG_EVENT'] = GS_START
            log['STATUS'] = GS_INFO
            log['SOURCE_NAME'] = full_name
            log['LOG_MESSAGE'] = ''
            write_execution_log(log)

            move_status_ok, ret_message = move_blob(bucket_name,
                                                    blob_name, 
                                                    step.TARGET_NAME)

            if move_status_ok :
                success_counter += 1
                log['STATUS'] = GS_INFO
                log['LOG_MESSAGE'] = 'archive success'
            else:
                log['STATUS'] = GS_ERROR
                log['LOG_MESSAGE'] = str(ret_message)
                status_ok = False

            log['REQUEST_LOG_EVENT'] = GS_END
            write_execution_log(log)

        if len(blobs) > 1:
            log['SOURCE_TYPE'] = ''
            log['SOURCE_NAME'] = step.SOURCE_NAME
            log['TARGET_TYPE'] = ''
            log['TARGET_NAME'] = ''

            if success_counter == len(blobs):
                log['STATUS'] = GS_INFO
                log['LOG_MESSAGE'] = 'all files archived'
            else:
                log['STATUS'] = GS_ERROR
                log['LOG_MESSAGE'] = 'file archives incomplete'
                status_ok = False

            log['REQUEST_LOG_EVENT'] = GS_END
            write_execution_log(log)

    #-------------------------------------------------------------------------

    elif step.TYPE == 'FILE' and step.SUBTYPE == 'DELETE':

        delete_status_ok = True
        success_counter = 0

        blobs = get_blobs(step.SOURCE_NAME)
        bucket_name = blobs[0].bucket.name

        for i in range(len(blobs)):

            blob_name = blobs[i].name
            full_name = blobs[i].bucket.name + '/' + blobs[i].name
            log['SOURCE_NAME'] = full_name
            log['REQUEST_LOG_EVENT'] = GS_START
            log['STATUS'] = GS_INFO
            log['LOG_MESSAGE'] = ''
            write_execution_log(log)

            delete_status_ok = delete_blob(blobs[i])

            if delete_status_ok:
                success_counter += 1
                log['STATUS'] = GS_INFO
                log['LOG_MESSAGE'] = 'delete success'
            else:
                log['STATUS'] = GS_ERROR
                log['LOG_MESSAGE'] = 'delete fail'
                status_ok = False

            log['REQUEST_LOG_EVENT'] = GS_END
            write_execution_log(log)

        if len(blobs) > 1:
            log['SOURCE_TYPE'] = ''
            log['SOURCE_NAME'] = step.SOURCE_NAME
            log['TARGET_TYPE'] = ''
            log['TARGET_NAME'] = ''

            if success_counter == len(blobs):
                log['STATUS'] = GS_INFO
                log['LOG_MESSAGE'] = 'file deletions complete'
            else:
                log['STATUS'] = GS_ERROR
                log['LOG_MESSAGE'] = 'file deletions incomplete'
                status_ok = False

            log['REQUEST_LOG_EVENT'] = GS_END
            write_execution_log (log)

    #-------------------------------------------------------------------------

    elif step.TYPE == 'PROCEDURE' and step.SUBTYPE == 'LOAD':

        log['REQUEST_LOG_EVENT'] = GS_START
        log['STATUS'] = GS_INFO
        write_execution_log(log)

        load_status_ok = True
        return_message = ''

        load_status_ok, return_message = call_procedure(step.STEP_NAME,
                                                       log['REQUEST_ID'],
                                                       step.LOAD_MODE,
                                                       step.CONFIGURATION)

        if load_status_ok :
            log['STATUS'] = GS_INFO
            log['LOG_MESSAGE'] = 'procedure success'
        else:
            log['STATUS'] = GS_ERROR
            log['LOG_MESSAGE'] = str(return_message)
            status_ok = False

        log['REQUEST_LOG_EVENT'] = GS_END
        write_execution_log(log)

    #-------------------------------------------------------------------------

    else:

        log['STATUS'] = GS_ERROR
        log['LOG_MESSAGE'] = 'invalid metadata step'
        status_ok = False
        write_execution_log(log)

    return status_ok


#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

def load_file(event, context):

    # initial setup

    global GS_PROJECT_ID
    global GS_META_TABLE
    global G_STG_CLIENT
    global G_BQ_CLIENT
    global GS_EXECUTION_LOG
    global GD_LOG_DICT
    global GS_INFO
    global GS_ERROR
    global GS_START
    global GS_END
    global GS_ABORT
    global GN_SLEEP_SECS
    global GS_RECEIVE_LOG
    global GB_VERBOSE

    GS_PROJECT_ID = os.environ.get('GCP_PROJECT', '')

    GS_META_TABLE = os.environ.get('GS_META_TABLE', 
                                   'FIN_ADMIN_UTILITY.JOB_METADATA')

    GS_EXECUTION_LOG = os.environ.get('GS_EXECUTION_LOG',
                                      'FIN_ADMIN_UTILITY.JOB_EXECUTION_LOG')

    GN_SLEEP_SECS = os.environ.get('GN_SLEEP_SECS', 180)
    
    # GB_VERBOSE = False
    GB_VERBOSE = os.environ.get('GB_VERBOSE', False)
    

    G_STG_CLIENT = storage.Client()
    G_BQ_CLIENT = bigquery.Client()
    G_BQ_CLIENT.project = GS_PROJECT_ID

    GS_INFO = 'INFO'
    GS_ERROR = 'ERROR'
    GS_ABORT = 'ABORT'
    GS_START = 'START'
    GS_END = 'END'

    GD_LOG_DICT = define_log_structure()

    time_utc, time_local = get_time_now()

    if GB_VERBOSE:
        print('metadata table: ' + GS_META_TABLE)
        print('sleep secs: ' + str(GN_SLEEP_SECS))

    #.........................................................................

    bucket_name = event['bucket']
    blob_name = event['name']
    file_name = blob_name.split('/')[-1]

    if GB_VERBOSE:
        print('bucket_name: ' + bucket_name)
        print('blob_name: ' + blob_name) 

    status_ok = True

    # fetch metadata and identify job matching this file_name
    ret_val, meta_df = get_all_metadata(GS_META_TABLE)
    if ret_val != '':
        print('error getting metadata')
        status_ok = False
    else:
        search_name = bucket_name + '/' + blob_name
        status_ok, trigger_dict, job_meta_df = get_job(meta_df, search_name)


    if status_ok:

        cfg_str = trigger_dict['CONFIGURATION']
        cfg_file = get_json_item(cfg_str, 'filename')

        # CONTROLLING TRIGGER TEST
        if cfg_file == '' or match(file_name,cfg_file):

            if GB_VERBOSE:
                print('job started with: ' + file_name)

            job_log = {}
            job_log['JOB_INSTANCE'] = str(uuid.uuid4())
            job_log['REQUEST_LOG_EVENT'] = 'JOB START'
            job_log['REQUEST_ID'] = get_local_time_now()
            job_log['STATUS'] = GS_INFO
            job_log['JOB_NAME'] = trigger_dict['JOB_NAME']
            job_log['JOB_VERSION'] = trigger_dict['JOB_VERSION']
            job_log['STEP'] = trigger_dict['STEP']
            job_log['SOURCE_NAME'] = trigger_dict['SOURCE_NAME']
            write_execution_log (job_log)

            # STEP SEQUENCE
            for i in range(len(job_meta_df)):
                if status_ok and active(job_meta_df.iloc[i].CONFIGURATION):
                    step_df = job_meta_df.iloc[i].copy()
                    status_ok = action_step(step_df, job_log)

            job_log['REQUEST_LOG_EVENT'] = 'JOB END'

            if status_ok:
                job_log['LOG_MESSAGE'] = 'job success'
            else:
                job_log['STATUS'] = GS_ERROR
                job_log['LOG_MESSAGE'] = 'job fail'
            write_execution_log(job_log)
