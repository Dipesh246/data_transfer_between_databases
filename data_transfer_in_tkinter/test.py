import datetime
from connection import connect_postgre, connect_mysql

src_db = connect_mysql()

with src_db.cursor() as src_cur:
    src_cur.execute("SELECT  cuer_createddatead,cuer_createddatebs FROM in_cuer_customer")
    data = src_cur.fetchall()

    # print(data)
    for d in data:
        yerad=int(d[0][:4])
        
        yerbs=int(d[1][:4])
        mnth=int(d[0][4:7]) 
        if mnth >= 4:
            fiscalbs=f"{yerbs}/{yerbs+1}"
            fiscalad=f"{yerad}/{yerad+1}"
            print(f"{fiscalad} {fiscalbs}")
        else:
            fiscalbs =f"{yerbs-1}/{yerbs}"
            fiscalad =f"{yerad-1}/{yerad}"
            print(f"{fiscalad} {fiscalbs}")
    

# dest_db = connect_postgre()

# with dest_db.cursor() as dest_cur:
#     query=f"""INSERT INTO dummy_data(id,date_ad)"""
