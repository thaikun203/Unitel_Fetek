import pendulum
from airflow.providers.oracle.hooks.oracle import OracleHook


def generate_info(delta_days, **kwargs):
    ti = kwargs['ti']
    # Thời gian thực thi trong Airflow đang bị thiếu mất 1 ngày
    execution_date = kwargs['data_interval_start'].add(days=1)

    execution_date = execution_date.subtract(days=delta_days)
    pre_date = execution_date.subtract(days=delta_days+1)
    next_date = execution_date.add(days=delta_days+1)
    
    pre_date = pre_date.strftime("%Y-%m-%d")
    execution_date = execution_date.strftime("%Y-%m-%d")
    next_date = next_date.strftime("%Y-%m-%d")

    # XCom Push
    ti.xcom_push(key='start_time', value=pendulum.now("Asia/Vientiane"))
    ti.xcom_push(key='pre_date', value=pre_date)
    ti.xcom_push(key='execution_date', value=execution_date)
    ti.xcom_push(key='next_date', value=next_date)

def get_execution(**kwargs):
    return kwargs['ti'].xcom_pull(key="execution_date").replace("-","")

# Hàm callback khi task gặp lỗi
def failure_callback(context):
    ti = context['task_instance']
    exception_message = str(context['exception']).split('\n')[0]
    error_message = f"Task {ti.task_id} failed. Error: {exception_message}"
    # Đẩy thông tin lỗi vào XCom
    ti.xcom_push(key='error_message', value=error_message)


# Hàm callback khi task thực thi thành công
def success_callback(context):
    ti = context['task_instance']
    ti.xcom_push(key='source_success_at', value=pendulum.now("Asia/Vientiane"))


def task_failure(**kwargs):
    raise Exception("This task encountered an error.")


def exec_procedure_STAGING(table_name, conn_id, **kwargs):
    oracle_hook = OracleHook(oracle_conn_id=conn_id)
    execution_date = kwargs['ti'].xcom_pull(key='execution_date').replace("-", "")
    
    sql = f"BEGIN STAGING.PR_{table_name}('{execution_date}'); END;"
    
    sql_count = f"SELECT count(*) FROM STAGING.{table_name}"
    
    count_result = oracle_hook.get_first(sql_count)
    total_rows = count_result[0] if count_result else 0
    kwargs["ti"].xcom_push(key="total_row", value=total_rows)
    
    oracle_hook.run(sql)

    
    
