import datetime
from psycopg2 import Error


from connection import connect_mysql,connect_postgre

def import_mysql_data():
    src_db = connect_mysql()
    

    src_db.autocommit= True
    

    with src_db.cursor() as cur:

        cur.execute("SELECT tile_titleid,tile_titlenameshort,tile_titlename,tile_createddatead,tile_createddatebs FROM gl_tile_title")
        data = cur.fetchall()
        print(len(data),data)
        cur.close()

        
    src_db.close()
    return data
def export_mysql_data(src_data):
    dest_db = connect_postgre()
    dest_db.autocommit=True

    # src_data = import_msql_data()
    try:
        with dest_db.cursor() as cur:
            # d= list(src_data)
            # del(d[0])
            # print(d)
        
            cur.execute('''INSERT INTO dummy_data(id,title, name, date_ad, date_bs)
                            VALUES(%s,%s,%s,%s,%s)''',src_data)

    except Error as e:
        dest_db.rollback()
        return e
        

        # for data in src_data:
        #     # a= list(d)
        #     # a[4] = a[4]+a[6]
        #     # print("\n",a)
        #     # a[4] = datetime.datetime.strptime(a[4],"%Y-%m-%d%H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
        #     # del a[-2]
        #     # a[3] = bool(a[3])
            
        #     # print("\n",a)
            
        #     cur.execute('''INSERT INTO dummy_data(id,title, name, date_ad, date_bs)
        #                     VALUES(%s,%s,%s,%s,%s)''',data)

        cur.close()
        dest_db.close()  