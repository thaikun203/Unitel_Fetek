from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import copy



########## INSERT LOG ##########
def log_table(prompt, log_table_name):
    # Lấy hook từ Airflow connection
    postgres_hook = PostgresHook(postgres_conn_id='metadata_db')

    insert_query = f"""
    INSERT INTO public.{log_table_name} (
        data_date, project_name, job_name, job_type, table_name,
        start_time, end_time, duration, total_row, status_name, 
        status_flag, src_system, schema_nm, source_success_at, error_description
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    data = (
        prompt.get('data_date'), prompt.get('project_name'), prompt.get('job_name'), 
        prompt.get('job_type'), prompt.get('table_name'), prompt.get('start_time'), 
        prompt.get('end_time'), prompt.get('duration'), prompt.get('total_row'), 
        prompt.get('status_name'), prompt.get('status_flag'), prompt.get('src_system'), 
        prompt.get('schema_nm'), prompt.get('source_success_at'), prompt.get('error_description')
    )

    try:
        postgres_hook.run(insert_query, parameters=data)
        response = 'Insert Log Successfully!'
    except Exception as e:
        response = f'Lỗi không insert được: {e}'
    return response


data_template = {
        'data_date': None,
        'project_name': None,
        'job_name': None,
        'job_type': None,
        'table_name': None,
        'start_time': None,
        'end_time': None,
        'duration': None,
        'total_row': None,
        'status_name': None,
        'status_flag': None,
        'src_system': None,
        'schema_nm': None,
        'source_success_at': None,
        'error_description': None
    }


def insert_log(project_name, table_name, job_type, src_system, schema_nm, log_table_name, **kwargs):
    data = copy.deepcopy(data_template)
    # Lấy lỗi từ XCom (nếu có)
    error_message = kwargs['ti'].xcom_pull(key='error_message')
    print(error_message)
    
    if error_message == None:
        data['status_name'] = "Success"
        data['status_flag'] = 1
    else:
        data['status_name'] = "Error"
        data['status_flag'] = 0

    data['data_date'] = kwargs['ti'].xcom_pull(key='execution_date')
    data['project_name'] = project_name
    data['job_name'] = kwargs['dag'].dag_id
    data['job_type'] = job_type
    data['table_name'] = table_name
    data['start_time'] = kwargs['ti'].xcom_pull(key='start_time')
    data['end_time'] = pendulum.now("Asia/Vientiane")
    data['duration'] = (data['end_time'] - data['start_time']).total_seconds()
    data['total_row'] = kwargs['ti'].xcom_pull(key='total_row')
    data['src_system'] = src_system
    data['schema_nm'] = schema_nm
    data['source_success_at'] = kwargs['ti'].xcom_pull(key='source_success_at')
    data['error_description'] = error_message

    log_table(data, log_table_name)