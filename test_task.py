import pandas as pd
import numpy as np
import os, sys
# import xlrd

import time, datetime

import logging
import zipfile
import argparse

from sqlalchemy import create_engine
import sqlite3 

logger = None

def init_logger():
    logger = logging.getLogger('Test_task')
    logger.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    strfmt = '%(asctime)s - %(levelname)s > %(message)s'
    datefmt = '%H:%M:%S'
    formatter = logging.Formatter(fmt=strfmt, datefmt=datefmt)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    return logger

def init_serv_funtions():
    logger = init_logger()
    return logger

def test_inputs(fn_path, fn, db_path):
    test_inputs_rez = True
    # if not os.path.exists(os.path.join(os.curdir, fn_path, fn)):
    if not os.path.exists(os.path.join(fn_path, fn)):
        logger.error(f"Test file '{fn}' not found in '{fn_path}'")
        test_inputs_rez = False
    # if not os.path.exists(os.path.join(os.curdir, db_path)) :
    if not os.path.exists(db_path) :
        logger.error(f"DB folder '{db_path}' not found")
        test_inputs_rez = False
    # elif not os.path.isdir(os.path.join(os.curdir, db_path)):
    elif not os.path.isdir(db_path):
        logger.error(f"DB folder '{db_path}' is not dir")
        test_inputs_rez = False
    
    return test_inputs_rez

def read_test_file(fn_path, fn, header = [0,1,2], index_col = [0,1]):
    df = pd.read_excel(os.path.join(fn_path, fn), header = header, index_col = index_col)
    logger.info(f"DataFrame shape: {str(df.shape)}")
    return df

def display_df_head(df):
    if not (type(df) == pd.core.frame.DataFrame): return
    logger.info(f"DataFrame Columns: {str(df.columns)}")
    s_lst = []
    for i_row, row in df.head().iterrows():
        # print(i_row, row)
        s_lst.append(f"i_row, {str(row)}\n")
    logger.info(f"DataFrame Head:\n {''.join(s_lst)}")

def convert_multiindex(df, columns_to_rename = {'variable_0': 'type', 'variable_1': 'indicator', 'variable_2': 'date'}):
    '''multiindex -> single index
    для заданного формата таблицы в Excel
    '''
    df_02 = df.melt(value_name='value') 
    df_02.rename(columns = columns_to_rename, inplace=True)

    return df_02

def create_sqlite_db(db_path, db_name, df, table_name = 'test_table'):
    try:
        engine = create_engine(f"sqlite:///{os.path.join(db_path, db_name)}", echo=True)
        df.to_sql(table_name, con=engine)
    except Exception as err:
        logger.error("Уже раз базу сделали хватит")
        engine = None
    return engine

def add_totals(db_path, db_name, table_name = 'test_table'):
    conn = sqlite3.connect(os.path.join(db_path, db_name))
    cur = conn.cursor()

    try:
        sql_str_alter = f"ALTER TABLE {table_name} ADD total BIGINT"
        cur.execute(sql_str_alter)
        conn.commit()
    except Exception as err:
        logger.error("Уже раз добавили поле 'total'")
        

    sql_str_create = '''CREATE TABLE temp_table ("index" BIGINT, 'sum_value' BIGINT)'''
    cur.execute(sql_str_create)
    conn.commit()

    sql_str_insert = f'''insert into temp_table ("index", sum_value)
    SELECT	"index", SUM(value)  
    OVER (ORDER BY type asc, date asc) as total 
    FROM {table_name} ORDER BY type, date'''
    cur.execute(sql_str_insert)
    conn.commit()

    sql_str_update = f'''UPDATE {table_name} 
    SET total = (SELECT sum_value
    FROM temp_table
    WHERE {table_name}."index" = temp_table."index")'''
    cur.execute(sql_str_update)
    conn.commit()

    str_sql_select_test = f'SELECT * FROM {table_name}'
    for it in conn.execute(str_sql_select_test):
        print(it)

    str_sql_drop = "DROP TABLE temp_table"
    cur.execute(str_sql_drop)
    conn.commit()
    
    logger.info('Totals have been added')

def execute_test_task(fn_path = '/data/',
                      fn = 'Приложение_к_заданию_бек_разработчика.xlsx',
                      db_path = '/db/',
                      db_name = 'test_db.sqlite',
                      table_name = 'test_table'
    ):
  
    global logger
    logger = init_serv_funtions()
    if not test_inputs(fn_path, fn, db_path):
        logger.info('Errors were found. Program is over')
        sys.exit(2)
    df = read_test_file(fn_path, fn)
    # display(df.head())
    # display_df_head(df)

    df_02 = convert_multiindex(df)
    engine = create_sqlite_db(db_path, db_name, df_02, table_name)

    add_totals(db_path, db_name, table_name)

def parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument('--fn_path', '-df', type=str, default = '/data/',
        help="File path")
    parser.add_argument('--fn', '-fn', type=str, default = 'Приложение_к_заданию_бек_разработчика.xlsx',
        help="Test File '*.xlsx'")
    parser.add_argument('--db_path', '-dd', type=str, default = '/db/',
        help="DB path")
    parser.add_argument('--db_name', '-dn', type=str, default = 'test_db.sqlite',
        help="DB name")
    parser.add_argument('--table_name', '-tn', type=str, default='test_table',
        help="table_name")
    opt = parser.parse_args()
    return opt


if __name__ == '__main__':
    if len(sys.argv) > 1: 
        opt = parse_opt()
        execute_test_task(**vars(opt))
    else:
        execute_test_task()        




