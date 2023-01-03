import os
import sys
import psycopg2
import numpy as np


def connect_db():
    while True:
        #check if db password file exists, otherwise manual entry
        if os.path.isfile('./db_password.txt'):
            with open('./db_password.txt', 'r') as f:
                password = f.read()
        else:
            password = input("Input database password: ")
        try:
            conn = psycopg2.connect("host=127.0.0.1 dbname=slr_final user=postgres port=5432 password={}".format(password))
            print('[+] Connection established')
            return conn
        except Exception as e:
            print(f"Error {e}. Try again.")
            sys.exit(1)

def query_result_return(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        # copy query to CSV output
        # with open(self.csv_output_path, 'w', encoding='utf-8') as f:
        #     cursor.copy_expert(outputquery, f)
        # print("Export file created at: {}".format(self.csv_output_path))
        return np.array(rows)