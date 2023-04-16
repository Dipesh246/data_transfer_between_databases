import pandas as pd
import json
from connection import connect_postgre

with open('/home/dipesh/Downloads/fiscal_session_bs.json','r') as f:
    data = json.load(f)


conn = connect_postgre()

for item in data:
    pk = item['pk']
    session_full = item['fields']['session_full']
    
    session_short = item['fields']['session_short']
    
    created_date_ad = item['fields']['created_date_ad']
    
    created_date_bs = item['fields']['created_date_bs']
    
    created_by = item['fields']['created_by']
    
    device_type = item['fields']['device_type']
    
    app_type = item['fields']['app_type']
    
    data_list=[pk,session_full,session_short,created_date_ad,created_date_bs,created_by,device_type,app_type]   
    
    
    cur = conn.cursor()
    print("before inserting:",data_list)
    cur.execute("""INSERT INTO branch_khasauliphc.core_app_fiscalsessionbs(id,session_full,session_short,created_date_ad,created_date_bs,created_by_id,device_type_id,app_type_id)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s) returning id""",data_list)
    print(cur.fetchone()[0])
    conn.commit()
    print("after inserting:",data_list)