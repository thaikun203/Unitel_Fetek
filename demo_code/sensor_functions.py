from airflow.sensors.sql import SqlSensor


########## Check Sensor ##########
def check_sensor(task_id, conn_id, sql, timeout=300, poke_interval=30, mode='poke', on_success_callback=None):
    check_conn_task = SqlSensor(
    task_id=task_id,
    conn_id=conn_id, 
    sql=sql,
    timeout=timeout,
    poke_interval=poke_interval,
    mode=mode,
    on_success_callback=on_success_callback
)
    return check_conn_task


########## Check count source table ##########
def render_sql_oracle(source_table, date_field, **kwargs):
    ti = kwargs['ti']
    pre_date = ti.xcom_pull(key='pre_date')
    execution_date = ti.xcom_pull(key='execution_date')
    next_date = ti.xcom_pull(key='next_date')

    sql = f"""
        WITH day_n AS (
            SELECT COUNT(*) count_n
            FROM {source_table}
            WHERE {date_field} >= TO_DATE('{execution_date}', 'yyyy-mm-dd')
            AND {date_field} < TO_DATE('{next_date}', 'yyyy-mm-dd')
        ), day_n_1 AS (
            SELECT COUNT(*) count_n_1
            FROM {source_table}
            WHERE {date_field} >= TO_DATE('{pre_date}', 'yyyy-mm-dd')
            AND {date_field} < TO_DATE('{execution_date}', 'yyyy-mm-dd')
        )
        SELECT 
            CASE WHEN count_n < count_n_1 / 2 THEN 0 
            ELSE 1 END AS status
        FROM day_n
        CROSS JOIN day_n_1
    """
    print(f"Rendered SQL: {sql}")

    ti.xcom_push(key='render_sql_oracle', value=sql)


def render_sql_mariadb(source_table, date_field, **kwargs):
    ti = kwargs['ti']
    pre_date = ti.xcom_pull(key='pre_date')
    execution_date = ti.xcom_pull(key='execution_date')
    next_date = ti.xcom_pull(key='next_date')

    sql = f"""
        WITH day_n AS (
            SELECT COUNT(*) count_n
            FROM {source_table}
            WHERE {date_field} >= '{execution_date}'
            AND {date_field} < '{next_date}'
        ), day_n_1 AS (
            SELECT COUNT(*) count_n_1
            FROM {source_table}
            WHERE {date_field} >= '{pre_date}'
            AND {date_field} < '{execution_date}'
        )
        SELECT 
            CASE WHEN count_n < count_n_1 / 2 THEN 0 
            ELSE 1 END AS status
        FROM day_n
        CROSS JOIN day_n_1
    """
    print(f"Rendered SQL: {sql}")

    ti.xcom_push(key='render_sql_mariadb', value=sql)


def render_sql_check_logs(job_names, **kwargs):
    ti = kwargs['ti']
    execution_date = ti.xcom_pull(key='execution_date')

    count = len(job_names.split(','))

    sql = f"""
        select 
            case when count(*) >= {count} then true 
                else false 
            end 
        from log_etl_table 
        where job_name in {job_names}
            and data_date = '{execution_date}'
            and status_flag = 1
    """
    print(f"Rendered SQL: {sql}")

    ti.xcom_push(key='render_sql_check_logs', value=sql)