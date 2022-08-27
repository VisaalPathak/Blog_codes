from airflow.models import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator

from datetime import datetime
from sqlalchemy import create_engine

default_args = {'start_date': datetime(2022, 8, 1)}

with DAG('simpel_MySQL_ops', default_args=default_args, catchup=False) as dag:
    create_database = MySqlOperator(task_id='create_schema',
                                    mysql_conn_id='mysq_default', sql='''create database if not exists new_db;''')

    create_table = MySqlOperator(task_id='create_table', 
                                 mysql_conn_id='mysq_default',
                                 sql='''use new_db;
                                 CREATE TABLE IF NOT EXISTS `new_table` (
                                        `id` INT NOT NULL AUTO_INCREMENT,
                                        `name` VARCHAR(50) DEFAULT NULL,
                                        primary key(id)
                                    );''')

    insert_data = MySqlOperator(task_id = 'insert_data',
                                mysql_conn_id = 'mysq_default',
                                sql = '''use new_db;
                                            insert into new_table values (1,"visaal"),(2,"bishal");''')

create_database >> create_table >> insert_data

