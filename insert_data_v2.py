#https://github.com/Strata-Scratch/api-youtube/blob/main/importing_df_to_db_final.ipynb

import pymysql
import pandas as pd

def mysqlconnect():
    # To connect MySQL database
    conn = pymysql.connect(
        host='localhost',
        user='root', 
        password = "harika123",
        db='groc_store',
        )
      
    #cur = conn.cursor()
    return conn

def select_table(schema,tablename):
    conn = mysqlconnect()
    cur = conn.cursor()
    select_query ="""select * from  {}.{}""".format(schema,tablename)
    cur.execute(select_query)
    output = cur.fetchall()
    for i in output:
        print(i)
    # To close the connection
    conn.commit()
    cur.close()
    conn.close()

data= {'Name':['banana','greaps','apple','orenge'],
       'uam_id':[2,2,2,2],
       'price_per_unit':[60,80,100,60]}
df = pd.DataFrame(data)

records_to_inser = df.values.tolist()
print(records_to_inser)


def insert_table(records,schema,tablename):
    conn = mysqlconnect()
    cur = conn.cursor()
    insert_query =""" insert into {}.{} (name,uam_id,price_per_unit)  values    (%s, %s, %s)""".format(schema,tablename)
    print(insert_query)
    cur.executemany(insert_query,records)
    conn.commit()
    cur.close()
    conn.close()
    
def insert_into_table(curr, Name, uam_id, price_per_unit):
    conn = mysqlconnect()
    cur = conn.cursor()
    insert_into_videos = ("""INSERT INTO groc_store.Product (name,uam_id,price_per_unit)
    VALUES(%s,%d,%d);""")
    row_to_insert = (Name, uam_id, price_per_unit)
    cur.execute(insert_into_videos, row_to_insert)
    conn.commit()
    cur.close()
    conn.close()
"""
cursor.executemany(insert_query,records_to_inser)
conn.commit()
conn.close()"""
# Driver Code
if __name__ == "__main__" :
    #insert_table(records_to_inser,'groc_store','Product')
    select_table('groc_store','Product')