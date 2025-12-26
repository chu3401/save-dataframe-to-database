# -*- coding: utf-8 -*-
"""
Created on Wed Dec 24 16:22:04 2025

@author: C
"""

import mysql.connector
import pandas as pd

class data2mysql:
    def __init__(self,df):
        self.df = df
    
    def save2sql(self):
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='password',
            database='db',
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()

        columns = self.df.columns.tolist()
        sql = f"""
        INSERT INTO news ({', '.join(columns)})
        values ({", ".join(["%s"]*len(columns))})
        """
        
        for i, row in self.df.iterrows():
            values = tuple(row[col] for col in columns)
            cursor.execute(sql, values)
            
        conn.commit()

if __name__ == "__main__":    
    df=pd.read_csv('yourcsv.csv')
    data2mysql(df)
    data2mysql.save2sql()

