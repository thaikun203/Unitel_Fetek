from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
import common.oracle_functions as oracle_functions
import common.sensor_functions as sensor_functions


def fact_append(dag, oracle_conn_id, source_table, date_field, procedure_name):
    """
    Task Group đồng bộ dữ liệu hàng ngày từ bảng source về ODS
    :param dag: DAG mà task group sẽ thuộc về
    :return: Task Group chứa các task
    """
    with TaskGroup("sync_data_daily", dag=dag) as sync_data_daily:
        # Task 1: Count số lượng records được fetch trong bảng sourec
        count_records = PythonOperator(
            task_id='count_records',
            python_callable=oracle_functions.count_records,
            op_kwargs = {
                'oracle_conn_id'    : oracle_conn_id,
                'table_name'        : source_table,
                'date_field'        : date_field
                },
        )

        # Mục đích: Test xem Webserver đã quét file chưa
        # dummy_task = DummyOperator(task_id='dummy_task')

        # Task 2: Query và Import dữ liệu vào bảng ODS
        trigger_proc_fa = PythonOperator(
            task_id='trigger_proc_fa',
            python_callable=oracle_functions.trigger_procedure,
            op_kwargs = {
                'procedure_name'    : procedure_name,
                'oracle_conn_id'    : oracle_conn_id,
                'param'             : 'Yes'
                },
        )

        count_records >> trigger_proc_fa
        
    return sync_data_daily

# có cả đồng bộ oracle rồi, thay source_type = 'oracle' là được
def sync_mariadb_daily(dag, source_type, source_conn_id, oracle_conn_id, source_table, table_name, date_field):
    """
    Task Group đồng bộ dữ liệu hàng ngày từ bảng source về ODS
    :param dag: DAG mà task group sẽ thuộc về
    :return: Task Group chứa các task
    """
    with TaskGroup("sync_data_daily", dag=dag) as sync_data_daily:
        # Task 1: Truncate Partition ngày thực thi tại bảng ODS
        truncate_parititon_task = PythonOperator(
            task_id='truncate_parititon',
            python_callable=oracle_functions.truncate_parititon,
            op_kwargs = {
                'oracle_conn_id'    : oracle_conn_id,
                'table_name'        : table_name
                },
        )

        # Mục đích: Test xem Webserver đã quét file chưa
        # dummy_task = DummyOperator(task_id='dummy_task')

        # Task 2: Query và Import dữ liệu vào bảng ODS
        query_and_import_task = PythonOperator(
            task_id='query_and_import',
            python_callable=oracle_functions.query_and_import,
            op_kwargs = {
                'source_type'   : source_type,
                'source_conn_id': source_conn_id,
                'oracle_conn_id': oracle_conn_id,
                'source_table'  : source_table,
                'table_name'    : table_name,
                'date_field'    : date_field,
                },
        )

        truncate_parititon_task >> query_and_import_task
        
    return sync_data_daily
def sync_oracle_snapshot(dag, source_type, source_conn_id, oracle_conn_id, source_table, table_name, date_field):
    """
    Task Group đồng bộ dữ liệu hàng ngày từ bảng source về ODS
    :param dag: DAG mà task group sẽ thuộc về
    :return: Task Group chứa các task
    """
    with TaskGroup("sync_data_daily", dag=dag) as sync_data_daily:
        # Task 1: Truncate Partition ngày thực thi tại bảng ODS
        truncate_parititon_task = PythonOperator(
            task_id='truncate_table',
            python_callable=oracle_functions.truncate_table,
            op_kwargs = {
                'oracle_conn_id'    : oracle_conn_id,
                'table_name'        : table_name
                },
        )

        # Mục đích: Test xem Webserver đã quét file chưa
        # dummy_task = DummyOperator(task_id='dummy_task')

        # Task 2: Query và Import dữ liệu vào bảng ODS
        query_and_import_task = PythonOperator(
            task_id='query_and_import',
            python_callable=oracle_functions.query_and_import_snapshot,
            op_kwargs = {
                'source_type'   : source_type,
                'source_conn_id': source_conn_id,
                'oracle_conn_id': oracle_conn_id,
                'source_table'  : source_table,
                'table_name'    : table_name,
                'date_field'    : date_field,
                },
        )

        truncate_parititon_task >> query_and_import_task
        
    return sync_data_daily


#### Lucky_money_detail

def sync_oracle_snapshot1(dag, source_type, source_conn_id, oracle_conn_id, source_table, table_name):
    """
    Task Group đồng bộ dữ liệu hàng ngày từ bảng source về ODS
    :param dag: DAG mà task group sẽ thuộc về
    :return: Task Group chứa các task
    """
    with TaskGroup("sync_data_daily", dag=dag) as sync_data_daily:
        # Task 1: Truncate Partition ngày thực thi tại bảng ODS
        truncate_parititon_task = PythonOperator(
            task_id='truncate_table',
            python_callable=oracle_functions.truncate_table,
            op_kwargs = {
                'oracle_conn_id'    : oracle_conn_id,
                'table_name'        : table_name
                },
        )

        # Mục đích: Test xem Webserver đã quét file chưa
        # dummy_task = DummyOperator(task_id='dummy_task')

        # Task 2: Query và Import dữ liệu vào bảng ODS
        query_and_import_task = PythonOperator(
            task_id='query_and_import',
            python_callable=oracle_functions.query_and_import_snapshot1,
            op_kwargs = {
                'source_type'   : source_type,
                'source_conn_id': source_conn_id,
                'oracle_conn_id': oracle_conn_id,
                'source_table'  : source_table,
                'table_name'    : table_name,
                
                },
        )

        truncate_parititon_task >> query_and_import_task
        
    return sync_data_daily



def check_data_oracle(dag, conn_id, source_table, date_field):
    """
    Task Group kiểm tra dữ liệu trong bảng source
    :param dag: DAG mà task group sẽ thuộc về
    :return: Task Group chứa các task
    """
    with TaskGroup("check_data_oracle", dag=dag) as check_data_oracle:
        
        # Task 4: Render script SQL
        render_sql_task = PythonOperator(
            task_id='render_sql_query',
            python_callable=sensor_functions.render_sql_oracle,
            op_kwargs = {
                'source_table': source_table,
                'date_field': date_field
            },
        )

        # Mục đích: Test xem Webserver đã quét file chưa
        # dummy_task = DummyOperator(task_id='dummy_task')

        # Task 5: SqlSensor kiểm tra dữ liệu
        check_count_task = sensor_functions.check_sensor(
            task_id='check_count_source',
            conn_id=conn_id,
            sql="{{ task_instance.xcom_pull(task_ids='check_data_oracle.render_sql_query', key='render_sql_oracle') }}",
        )

        render_sql_task >> check_count_task
        
    return check_data_oracle


def check_data_mariadb(dag, conn_id, source_table, date_field):
    """
    Task Group kiểm tra dữ liệu trong bảng source
    :param dag: DAG mà task group sẽ thuộc về
    :return: Task Group chứa các task
    """
    with TaskGroup("check_data_mariadb", dag=dag) as check_data_mariadb:
        
        # Task 4: Render script SQL
        render_sql_task = PythonOperator(
            task_id='render_sql_query',
            python_callable=sensor_functions.render_sql_mariadb,
            op_kwargs = {
                'source_table': source_table,
                'date_field': date_field
            },
        )

        # Mục đích: Test xem Webserver đã quét file chưa
        # dummy_task = DummyOperator(task_id='dummy_task')

        # Task 5: SqlSensor kiểm tra dữ liệu
        check_count_task = sensor_functions.check_sensor(
            task_id='check_count_source',
            conn_id=conn_id,
            sql="{{ task_instance.xcom_pull(task_ids='check_data_mariadb.render_sql_query', key='render_sql_mariadb') }}",
        )

        render_sql_task >> check_count_task
        
    return check_data_mariadb


def scd_type1(dag, staging_conn_id, ods_conn_id, procedure_staging, procedure_ods, staging_table):
    with TaskGroup("scd_type1", dag=dag) as scd_type1:

        trigger_proc_staging = PythonOperator(
            task_id='trigger_proc_staging',
            python_callable=oracle_functions.trigger_procedure,
            op_kwargs = {
                'procedure_name'    : procedure_staging,
                'oracle_conn_id'    : staging_conn_id,
                'param'             : 'Yes'
                },
        )

        count_staging = PythonOperator(
            task_id='count_staging',
            python_callable=oracle_functions.count_staging,
            op_kwargs = {
                'oracle_conn_id'    : staging_conn_id,
                'table_name'        : staging_table,
                },
        )

        # Mục đích: Test xem Webserver đã quét file chưa
        # dummy_task = DummyOperator(task_id='dummy_task')

        trigger_proc_ods = PythonOperator(
            task_id='trigger_proc_ods',
            python_callable=oracle_functions.trigger_procedure,
            op_kwargs = {
                'procedure_name'    : procedure_ods,
                'oracle_conn_id'    : ods_conn_id,
                },
        )

        trigger_proc_staging >> count_staging >> trigger_proc_ods
        
    return scd_type1

def fu_type(dag, staging_conn_id, ods_conn_id, procedure_staging, procedure_ods, staging_table):
    with TaskGroup("fu_type", dag=dag) as fu_type_group:  # Đổi tên để tránh nhầm lẫn
        trigger_proc_staging = PythonOperator(
            task_id='trigger_proc_staging',
            python_callable=oracle_functions.trigger_procedure,
            op_kwargs={
                'procedure_name': procedure_staging,
                'oracle_conn_id': staging_conn_id,
                'param': 'Yes',
            },
        )

        count_staging = PythonOperator(
            task_id='count_staging',
            python_callable=oracle_functions.count_staging,
            op_kwargs={
                'oracle_conn_id': staging_conn_id,
                'table_name': staging_table,
            },
        )

        trigger_proc_ods = PythonOperator(
            task_id='trigger_proc_ods',
            python_callable=oracle_functions.trigger_procedure,
            op_kwargs={
                'procedure_name': procedure_ods,
                'oracle_conn_id': ods_conn_id,
                'param': 'Yes',
            },
        )

        # Thiết lập thứ tự các task
        trigger_proc_staging >> count_staging >> trigger_proc_ods
        
    return fu_type_group 


def single_snapshot(dag, ods_conn_id, procedure_ods, ods_table):
    with TaskGroup("single_snapshot", dag=dag) as single_snapshot:

        count_staging = PythonOperator(
            task_id='count_staging',
            python_callable=oracle_functions.count_staging,
            op_kwargs = {
                'oracle_conn_id'    : ods_conn_id,
                'table_name'        : ods_table,
                },
        )

        # Mục đích: Test xem Webserver đã quét file chưa
        # dummy_task = DummyOperator(task_id='dummy_task')

        trigger_proc_ods = PythonOperator(
            task_id='trigger_proc_ods',
            python_callable=oracle_functions.trigger_procedure,
            op_kwargs = {
                'procedure_name'    : procedure_ods,
                'oracle_conn_id'    : ods_conn_id,
                },
        )

        count_staging >> trigger_proc_ods
        
    return single_snapshot


def check_logs_ods(dag, conn_id, job_names):
    """
    Task Group kiểm tra logs thực thi các DAG ETL dữ liệu từ SRC về ODS
    :param dag: DAG mà task group sẽ thuộc về
    :return: Task Group chứa các task
    """
    with TaskGroup("check_logs_ods", dag=dag) as check_logs_ods:
        
        # Task 4: Render script SQL
        render_sql_task = PythonOperator(
            task_id='render_sql_query',
            python_callable=sensor_functions.render_sql_check_logs,
            op_kwargs = {
                'job_names': job_names,
            },
        )

        # Mục đích: Test xem Webserver đã quét file chưa
        # dummy_task = DummyOperator(task_id='dummy_task')

        # Task 5: SqlSensor kiểm tra dữ liệu
        check_logs_task = sensor_functions.check_sensor(
            task_id='check_logs_task',
            conn_id=conn_id,
            sql="{{ task_instance.xcom_pull(task_ids='check_logs_ods.render_sql_query', key='render_sql_check_logs') }}",
        )

        render_sql_task >> check_logs_task
        
    return check_logs_ods


def fu_type_no_dblink(dag,source_conn_id,staging_conn_id,ods_conn_id,source_table, procedure_ods,staging_table,source_type,date_field1,date_field2):
    with TaskGroup("fu_type_no_dblink", dag=dag) as fu_type_group:
        query_and_import_stag = PythonOperator(
            task_id='query_and_import',
            python_callable=oracle_functions.query_and_import_fu,
            op_kwargs={
                'source_type': source_type,
                'source_conn_id':source_conn_id, 
                'oracle_conn_id':staging_conn_id, 
                'source_table':source_table,
                'table_name':staging_table, 
                'date_field1':date_field1,
                'date_field2':date_field2,
            },
        )
        count_staging = PythonOperator(
            task_id='count_staging',
            python_callable=oracle_functions.count_staging,
            op_kwargs={
                'oracle_conn_id': staging_conn_id,
                'table_name': staging_table,
            },
        )
        trigger_proc_ods = PythonOperator(
            task_id='trigger_proc_ods',
            python_callable=oracle_functions.trigger_procedure,
            op_kwargs={
                'procedure_name': procedure_ods,
                'oracle_conn_id': ods_conn_id,
                
            },
        )
        query_and_import_stag >> count_staging >> trigger_proc_ods
    return fu_type_group


