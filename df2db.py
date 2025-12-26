# -*- coding: utf-8 -*-
"""
Created on Wed Dec 24 16:22:04 2025

@author: C
"""

import mysql.connector
import pyodbc
import pandas as pd

class DataSave2SQL:
    def __init__(self, df:pd.DataFrame): #type hint
        self.df = df
    
    def save(self):
        raise NotImplementedError("save") #?

class DataSave2MySQL(DataSave2SQL):
    def __init__(self, df, host='localhost', user='root', password='password', database='db'):
        super().__init__(df)
        self.host = host
        self.user = user
        self.password = password
        self.database = database
    
    def save(self):
        conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        columns = self.df.columns.tolist()
        sql = f"""
        INSERT INTO news ({', '.join(columns)})
        VALUES ({', '.join(['%s']*len(columns))})
        """
        for _, row in self.df.iterrows():
            values = tuple(row[col] for col in columns)
            cursor.execute(sql, values)
        conn.commit()
        conn.close()
    
    def preprocessing(self):
        self.df = self.df.where(pd.notnull(self.df), None)
        
        
    
    

class DataSave2MSSQL(DataSave2SQL):
    def __init__(self, df, server='localhost', database='db', user='sa', password='password'):
        super().__init__(df)
        self.server = server
        self.database = database
        self.user = user
        self.password = password

    def save(self):
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={self.server};DATABASE={self.database};UID={self.user};PWD={self.password}'
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        columns = self.df.columns.tolist()
        sql = f"""
        INSERT INTO news ({', '.join(columns)})
        VALUES ({', '.join(['?']*len(columns))})
        """
        for _, row in self.df.iterrows():
            values = tuple(row[col] for col in columns)
            cursor.execute(sql, values)
        conn.commit()
        conn.close()

if __name__ == "__main__":    
    df=pd.read_csv('yourcsv.csv')
    
    saver:DataSave2SQL = DataSave2MySQL(df)
    #saver:DataSave2SQL = DataSave2MySQL(df)
    saver.save()
