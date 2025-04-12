from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

import pandas as pd
import pendulum
from datetime import datetime, timedelta



########## ORACLE ##########
def get_insert_query(oracle_hook, table_name, values):
    # Truy vấn tên trường và kiểu dữ liệu của bảng
    info_table = f"""
        SELECT COLUMN_ID,  COLUMN_NAME, DATA_TYPE
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = '{table_name}' 
        ORDER BY COLUMN_ID
    """

    df_info = oracle_hook.get_pandas_df(info_table)

    # Lấy List Cols của bảng ODS
    column_name_list = df_info['COLUMN_NAME'].to_list()

    # Lấy ra các trường có kiểu dữ liệu là DATE hoặc bắt đầu bằng Timestamp
    date_type = df_info[(df_info['DATA_TYPE'] == 'DATE')]
    # timestamp_type = df_info[df_info['DATA_TYPE'].str.startswith('TIMESTAMP')]

    # Thay đổi các tham số câu lệnh Insert về đúng với dạng đặc biệt DATE hoặc Timestamp
    values_list = values.split(", ")
    for column_id in date_type['COLUMN_ID']:
        index = column_id - 1 	
        values_list[index] = f"TO_DATE(:{column_id}, 'YYYY-MM-DD HH24:MI:SS')"

    # Với df có kiểu dữ liệu là datetime.datetime thì không cần thêm hàm TO_TIMESTAMP trong câu lệnh Insert. Nếu là str thì cần
    # for column_id in timestamp_type['COLUMN_ID']:
    #     index = column_id - 1 		
    #     values_list[index] = f"TO_TIMESTAMP(:{column_id}, 'YYYY-MM-DD HH24:MI:SS.FF')"

    values = ", ".join(values_list)

    # Tạo câu lệnh Insert
    insert_query = f"INSERT INTO {table_name} ({', '.join(column_name_list)}) VALUES ({values})"

    return insert_query


def query_and_import_new(source_type, source_conn_id, oracle_conn_id, source_table, table_name, date_field, **kwargs):
    ti = kwargs['ti']
    execution_date = ti.xcom_pull(key='execution_date')
    next_date = ti.xcom_pull(key='next_date')

    # B1: Query dữ liệu từ nguồn
    if source_type == 'mariadb':
        mysql_hook = MySqlHook(mysql_conn_id=source_conn_id)
        with mysql_hook.get_conn() as conn: 
            with conn.cursor() as cursor:
                query = f"""
                SELECT *
                FROM {source_table}
                """
                cursor.execute(query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

    elif source_type == 'oracle':
        oracle_hook = OracleHook(oracle_conn_id=source_conn_id)
        query = f"""
         SELECT *
                FROM {source_table}
                
        """
        result = oracle_hook.get_records(query)

        owner, table = source_table.upper().split(".") if "." in source_table else ("", source_table.upper())

        columns = [col[0] for col in oracle_hook.get_records(f"""
        SELECT column_name 
        FROM all_tab_columns 
        WHERE owner = '{owner}' AND table_name = '{table}'
        """)]
    else:
        raise ValueError("Sai source_type")

    # Tạo DataFrame
    df = pd.DataFrame(result, dtype='object', columns=columns)

    # Thêm cột `tf_etl_at` để theo dõi thời gian ETL
    df['tf_etl_at'] = pendulum.now("Asia/Vientiane").strftime("%Y-%m-%d")

    # Lấy danh sách các cột DATE hoặc TIMESTAMP từ Oracle
    date_columns = oracle_hook.get_pandas_df(f"""
        SELECT COLUMN_NAME 
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = '{table_name}' 
              AND DATA_TYPE IN ('DATE', 'TIMESTAMP')
    """)['COLUMN_NAME'].tolist()

    # Chuyển đổi các cột DATE hoặc TIMESTAMP về định dạng chuỗi phù hợp
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)

    total_row = len(df)
    print(f'Số lượng records: {total_row}')
    ti.xcom_push(key='total_row', value=total_row)

    # Xử lý giá trị None để tránh lỗi khi insert vào Oracle
    df = df.where(pd.notnull(df), None)  
    data_tuples = [tuple(x) for x in df.values]

    # Tạo câu lệnh INSERT
    placeholders = ", ".join([":{}".format(i + 1) for i in range(len(columns))])
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    print(f'Table name: {table_name}')
    
    # B2: Import dữ liệu vào Oracle
    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, data_tuples)
            conn.commit()


########## SYNC DAILY ##########
def truncate_parititon(oracle_conn_id, table_name, **kwargs):
    ti = kwargs['ti']
    execution_date = ti.xcom_pull(key='execution_date')

    # Partition Name chuẩn cho tất cả các bảng Fact trong ODS: 'DATAyyymmdd'
    partition_name = 'DATA' + execution_date.replace('-', '')
    print(partition_name)

    oracle_hook = OracleHook(oracle_conn_id=oracle_conn_id)
    oracle_hook.run(f"ALTER TABLE {table_name} TRUNCATE PARTITION {partition_name}")

def truncate_table(oracle_conn_id, table_name, **kwargs):
    oracle_hook = OracleHook(oracle_conn_id=oracle_conn_id)
    oracle_hook.run(f"TRUNCATE TABLE {table_name}")

# Hàm này có thể sẽ tái sử dụng cho nhiều Task Group khác nên tạm thời thêm argument source_type vào làm điều kiện extract dữ liệu từ nhiều source khác nhau
def query_and_import(source_type, source_conn_id, oracle_conn_id, source_table, table_name, date_field, **kwargs):
    ti = kwargs['ti']
    ##### B1: Query dữ liệu trong bảng Source 
    execution_date = ti.xcom_pull(key='execution_date')
    next_date = ti.xcom_pull(key='next_date')

    if source_type == 'mariadb':
        mysql_hook = MySqlHook(mysql_conn_id=source_conn_id)
        with mysql_hook.get_conn() as conn: 
            with conn.cursor() as cursor:
                query = f"""
                SELECT *
                FROM {source_table}    
                WHERE {date_field} >= '{execution_date}' AND {date_field} < '{next_date}';
                """
                cursor.execute(query)

                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
    elif source_type == 'oracle':
        oracle_hook = OracleHook(oracle_conn_id=source_conn_id)
        query = f"""
        SELECT *
        FROM {source_table}
        WHERE {date_field} >= TO_DATE('{execution_date}', 'yyyy-mm-dd') AND {date_field} < TO_DATE('{next_date}', 'yyyy-mm-dd')
        """
        result = oracle_hook.get_records(query)

        owner, table = source_table.upper().split(".") if "." in source_table else ("", source_table.upper())

        columns = [col[0] for col in oracle_hook.get_records(f"""
        SELECT column_name 
        FROM all_tab_columns 
        WHERE owner = '{owner}' AND table_name = '{table}'
        """)]
    else:
        raise 'Sai source_type'
    
    df = pd.DataFrame(result, dtype='object', columns=columns)
    df['tf_etl_at'] = pendulum.now("Asia/Vientiane").strftime("%Y-%m-%d")
    total_row = len(df)
    print(f'Số lượng records: {total_row}')
    ti.xcom_push(key='total_row', value=total_row)

    ##### B2: Import dữ liệu vào bảng ODS
    # Chuyển đổi các giá trị 'None' thành None trong các Tuple
    columns = df.columns.tolist()
    df = df.where(pd.notnull(df), None)
    data_tuples = [tuple(map(lambda x: None if x == 'None' else x, x)) for x in df[columns].values]
    print(data_tuples[1])

    # Tạo chuỗi VALUES trong câu lệnh Insert
    values = ", ".join([":{}".format(i + 1) for i in range(len(columns))])

    print(f'Table name: {table_name}')

    oracle_hook = OracleHook(oracle_conn_id=oracle_conn_id)
    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Tạo câu lệnh Insert
            insert_query = get_insert_query(oracle_hook, table_name, values)
            print(insert_query)

            # Thực thi Executemany
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
# lấy theo kiểu fu
def query_and_import_fu(source_type, source_conn_id,oracle_conn_id,source_table, table_name, date_field1,date_field2, **kwargs):
    ti = kwargs['ti']
    ##### B1: Query dữ liệu trong bảng Source 
    execution_date = ti.xcom_pull(key='execution_date')
    next_date = ti.xcom_pull(key='next_date')

    if source_type == 'mariadb':
        mysql_hook = MySqlHook(mysql_conn_id=source_conn_id)
        with mysql_hook.get_conn() as conn: 
            with conn.cursor() as cursor:
                query = f"""
                SELECT *
                FROM {source_table}    
                WHERE {date_field1} >= '{execution_date}'
                OR {date_field2} >= '{execution_date}';
                """
                cursor.execute(query)

                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
    elif source_type == 'oracle':
        oracle_hook = OracleHook(oracle_conn_id=source_conn_id)
        with oracle_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                query = f"""
                SELECT *
                FROM {source_table}
                WHERE {date_field1} >= '{execution_date}'
                OR {date_field2} >= '{execution_date}';
                """

                print(query)
                cursor.execute(query)

                print('Đã query thành công!')

                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

    else:
        raise 'Sai source_type'
    
    df = pd.DataFrame(result, dtype='object', columns=columns)
    df['tf_etl_at'] = pendulum.now("Asia/Vientiane").strftime("%Y-%m-%d")
    total_row = len(df)
    print(f'Số lượng records: {total_row}')
    ti.xcom_push(key='total_row', value=total_row)

    ##### B2: Import dữ liệu vào bảng ODS
    # Chuyển đổi các giá trị 'None' thành None trong các Tuple
    columns = df.columns.tolist()
    df = df.where(pd.notnull(df), None)
    data_tuples = [tuple(map(lambda x: None if x == 'None' else x, x)) for x in df[columns].values]
    print(data_tuples[1])

    # Tạo chuỗi VALUES trong câu lệnh Insert
    values = ", ".join([":{}".format(i + 1) for i in range(len(columns))])

    print(f'Table name: {table_name}')

    oracle_hook = OracleHook(oracle_conn_id=oracle_conn_id)
    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                print("Table truncated successfully!")
            except Exception as e:
                print("Error during truncate:", str(e))
            
            # Tạo câu lệnh Insert
            insert_query = get_insert_query(oracle_hook, table_name, values)
            print(insert_query)

            # Thực thi Executemany
            
            cursor.executemany(insert_query, data_tuples)
            conn.commit()

def query_and_import_snapshot(source_type, source_conn_id, oracle_conn_id, source_table, table_name, date_field, **kwargs):
    ti = kwargs['ti']
    ##### B1: Query dữ liệu trong bảng Source
    execution_date = ti.xcom_pull(key='execution_date')
    
    if source_type == 'oracle':
        oracle_hook = OracleHook(oracle_conn_id=source_conn_id)
        with oracle_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                query = f"""
                SELECT *
                FROM {source_table}
                WHERE {date_field} < TO_DATE('{execution_date}', 'yyyy-mm-dd') + 1
                """

                print(query)
                cursor.execute(query)

                print('Đã query thành công!')

                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
    else:
        raise 'Sai source_type'
    
    df = pd.DataFrame(result, dtype='object', columns=columns)
    df['tf_etl_at'] = pendulum.now("Asia/Vientiane").strftime("%Y-%m-%d")
    total_row = len(df)
    print(f'Số lượng records: {total_row}')
    ti.xcom_push(key='total_row', value=total_row)

    ##### B2: Import dữ liệu vào bảng ODS
    # Chuyển đổi các giá trị 'None' thành None trong các Tuple
    columns = df.columns.tolist()
    df = df.where(pd.notnull(df), None)
    data_tuples = [tuple(map(lambda x: None if x == 'None' else x, x)) for x in df[columns].values]
    print(data_tuples[1])

    # Tạo chuỗi VALUES trong câu lệnh Insert
    values = ", ".join([":{}".format(i + 1) for i in range(len(columns))])

    print(f'Table name: {table_name}')

    oracle_hook = OracleHook(oracle_conn_id=oracle_conn_id)
    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Tạo câu lệnh Insert
            insert_query = get_insert_query(oracle_hook, table_name, values)
            print(insert_query)

            # Thực thi Executemany
            cursor.executemany(insert_query, data_tuples)
            conn.commit()





def count_records(oracle_conn_id, table_name, date_field, **kwargs):
    ti = kwargs['ti']
    execution_date = ti.xcom_pull(key='execution_date')
    next_date = ti.xcom_pull(key='next_date')

    oracle_hook = OracleHook(oracle_conn_id=oracle_conn_id)
    query = f'''
        SELECT COUNT(*) FROM {table_name}
        WHERE {date_field} >= TO_DATE('{execution_date}', 'yyyy-mm-dd') AND {date_field} < TO_DATE('{next_date}', 'yyyy-mm-dd')
    '''
    print(query)
    result = oracle_hook.get_first(query)
    # Lấy giá trị COUNT từ kết quả
    count = result[0]  
    print(f"Count result: {count}")
    kwargs['ti'].xcom_push(key='total_row', value=count)


def count_staging(oracle_conn_id, table_name, **kwargs):
    oracle_hook = OracleHook(oracle_conn_id=oracle_conn_id)
    query = f'SELECT COUNT(*) FROM {table_name}'
    print(query)
    result = oracle_hook.get_first(query)
    # Lấy giá trị COUNT từ kết quả
    count = result[0]  
    print(f"Count result: {count}")
    kwargs['ti'].xcom_push(key='total_row', value=count)


def trigger_procedure(procedure_name, oracle_conn_id, param=None, **kwargs):
    hook = OracleHook(oracle_conn_id=oracle_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    execution_date = kwargs['ti'].xcom_pull(key='execution_date').replace('-', '')
    print(f'Execution date: {execution_date}')
    
    try:
        if param is None:
            print(f'Call Procedure không có tham số')
            cursor.callproc(procedure_name)
        else:
            print(f'Call Procedure có tham số')
            cursor.callproc(procedure_name, [execution_date])
        conn.commit()
        print(f"Procedure {procedure_name} executed successfully.")
    except Exception as e:
        print(f"Error executing procedure: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def trigger_package_procedure(package_name, procedure_name, oracle_conn_id, param=None, **kwargs):
    hook = OracleHook(oracle_conn_id=oracle_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    execution_date = kwargs['ti'].xcom_pull(key='execution_date').replace('-', '')
    print(f'Execution date: {execution_date}')
    
    try:
        if param is None:
            print(f'Calling procedure {procedure_name} in package {package_name} without parameters.')
            cursor.callproc(f"{package_name}.{procedure_name}")  
        else:
            print(f'Calling procedure {procedure_name} in package {package_name} with parameters.')
            cursor.callproc(f"{package_name}.{procedure_name}", [execution_date])  
        conn.commit()
        print(f"Procedure {procedure_name} in package {package_name} executed successfully.")
    except Exception as e:
        print(f"Error executing procedure: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        
# Chạy Procedure với trừ đi 1 ngày 
def trigger_package_procedure_sub_1(package_name, procedure_name, oracle_conn_id, param=None, **kwargs):
    hook = OracleHook(oracle_conn_id=oracle_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    execution_date = kwargs['ti'].xcom_pull(key='execution_date')

    # Chuyển đổi execution_date thành đối tượng datetime
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d')

    # Trừ đi 1 ngày
    # execution_date = (execution_date - timedelta(days=1)).strftime('%Y%m%d')

    # print(f'Execution date (yesterday): {execution_date}')
    print(f'Execution date: {execution_date}')
    try:
        if param is None:
            print(f'Calling procedure {procedure_name} in package {package_name} without parameters.')
            cursor.callproc(f"{package_name}.{procedure_name}")  
        else:
            print(f'Calling procedure {procedure_name} in package {package_name} with parameters.')
            cursor.callproc(f"{package_name}.{procedure_name}", [execution_date])  
        conn.commit()
        print(f"Procedure {procedure_name} in package {package_name} executed successfully.")
    except Exception as e:
        print(f"Error executing procedure: {e}")
        raise
    finally:
        cursor.close()
        conn.close()




def get_insert_query1(oracle_hook, table_name, values):
    # Truy vấn tên trường và kiểu dữ liệu của bảng
    info_table = f"""
        SELECT COLUMN_ID, COLUMN_NAME, DATA_TYPE
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = '{table_name}' 
        ORDER BY COLUMN_ID
    """

    df_info = oracle_hook.get_pandas_df(info_table)

    # Lấy List Cols của bảng ODS
    column_name_list = df_info['COLUMN_NAME'].to_list()

    # Lấy ra các trường có kiểu dữ liệu là DATE
    date_type = df_info[df_info['DATA_TYPE'] == 'DATE']
    
    # Thay đổi các tham số câu lệnh Insert về đúng với dạng đặc biệt DATE
    values_list = values.split(", ")
    for column_id in date_type['COLUMN_ID']:
        index = column_id - 1
        # Sử dụng TO_DATE với định dạng đầy đủ cho datetime
        values_list[index] = f"TO_DATE(:{column_id}, 'YYYY-MM-DD HH24:MI:SS')"

    values = ", ".join(values_list)

    # Tạo câu lệnh Insert
    insert_query = f"INSERT INTO {table_name} ({', '.join(column_name_list)}) VALUES ({values})"

    return insert_query 

def query_and_import_snapshot1(source_type, source_conn_id, oracle_conn_id, source_table, table_name, **kwargs):
    ti = kwargs['ti']
    
    ##### B1: Query dữ liệu trong bảng Source
    if source_type == 'oracle':
        oracle_hook = OracleHook(oracle_conn_id=source_conn_id)
        
        # Lấy thông tin về các cột trong bảng nguồn
        columns_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM ALL_TAB_COLUMNS 
            WHERE TABLE_NAME = '{source_table.split('.')[-1]}'
            ORDER BY COLUMN_ID
        """
        
        columns_info = oracle_hook.get_pandas_df(columns_query)
        
        with oracle_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Tạo danh sách các cột trong câu SELECT
                select_columns = []
                for _, row in columns_info.iterrows():
                    column_name = row['COLUMN_NAME']
                    if row['DATA_TYPE'] == 'DATE':
                        select_columns.append(f"TO_CHAR(t.{column_name}, 'YYYY-MM-DD HH24:MI:SS') as {column_name}")
                    else:
                        select_columns.append(f"t.{column_name}")
                
                query = f"""
                SELECT 
                    {', '.join(select_columns)},
                    TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') as tf_etl_at
                FROM {source_table} t
                """

                print(query)
                cursor.execute(query)
                print('Đã query thành công!')

                result = cursor.fetchall()
                print('da fetch thanh cong')
                columns = [desc[0] for desc in cursor.description]
    else:
        raise ValueError('Sai source_type')
    
    df = pd.DataFrame(result, dtype='object', columns=columns)
    total_row = len(df)
    print(f'Số lượng records: {total_row}')
    ti.xcom_push(key='total_row', value=total_row)

    ##### B2: Import dữ liệu vào bảng ODS
    # Chuyển đổi các giá trị 'None' thành None trong các Tuple
    columns = df.columns.tolist()
    df = df.where(pd.notnull(df), None)
    data_tuples = [tuple(map(lambda x: None if x == 'None' else x, x)) for x in df[columns].values]
    print(data_tuples[1])

    # Tạo chuỗi VALUES trong câu lệnh Insert
    values = ", ".join([f":{i + 1}" for i in range(len(columns))])

    print(f'Table name: {table_name}')

    oracle_hook = OracleHook(oracle_conn_id=oracle_conn_id)
    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Tạo câu lệnh Insert
            insert_query = get_insert_query1(oracle_hook, table_name, values)
            print(insert_query)

            # Thực thi Executemany
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
def query_and_import_bak(source_type, source_conn_id,oracle_conn_id,source_table, table_name, date_field1):
    
    

    if source_type == 'mariadb':
        mysql_hook = MySqlHook(mysql_conn_id=source_conn_id)
        with mysql_hook.get_conn() as conn: 
            with conn.cursor() as cursor:
                query = f"""
                SELECT *
                FROM {source_table}    
                WHERE {date_field1} >= '2025-01-01'
                ;
                """
                cursor.execute(query)

                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
    
    else:
        raise 'Sai source_type'
    
    df = pd.DataFrame(result, dtype='object', columns=columns)
    df['tf_etl_at'] = pendulum.now("Asia/Vientiane").strftime("%Y-%m-%d")
    

    ##### B2: Import dữ liệu vào bảng ODS
    # Chuyển đổi các giá trị 'None' thành None trong các Tuple
    columns = df.columns.tolist()
    df = df.where(pd.notnull(df), None)
    data_tuples = [tuple(map(lambda x: None if x == 'None' else x, x)) for x in df[columns].values]
    print(data_tuples[1])

    # Tạo chuỗi VALUES trong câu lệnh Insert
    values = ", ".join([":{}".format(i + 1) for i in range(len(columns))])

    print(f'Table name: {table_name}')

    oracle_hook = OracleHook(oracle_conn_id=oracle_conn_id)
    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                print("Table truncated successfully!")
            except Exception as e:
                print("Error during truncate:", str(e))
            
            # Tạo câu lệnh Insert
            insert_query = get_insert_query(oracle_hook, table_name, values)
            print(insert_query)

            # Thực thi Executemany
            
            cursor.executemany(insert_query, data_tuples)
            conn.commit()

