import pandas as pd
def insert_query():
    insert_query ="""
    INSERT INTO {}
    ([Name],[Age])
    VALUES
    (?,?)
    """.format('schema.tablename')

data= {'Name':['Tom','nick','krish','Jack'],
       'Age':[20,21,23,19]}
df = pd.DataFrame(data)

records_to_inser = df.values.tolist()

if connectionStatus:
    cursor.executemany(insert_query,records_to_inser)
    conn.commit()
    conn.close()