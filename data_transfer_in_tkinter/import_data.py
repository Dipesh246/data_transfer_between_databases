import datetime

from connection import connect_mysql,connect_postgre

def import_data():
    src_db = connect_mysql()
    dest_db = connect_postgre()

    src_db.autocommit= True
    dest_db.autocommit=True

    with src_db.cursor() as cur:

        cur.execute("SELECT * FROM gl_usma_usermain")
        data = cur.fetchall()
        # print(data)
        cur.close()

        
    src_db.close()

    with dest_db.cursor() as cur:
        for d in data:
            # print("\n",d)
            a = list(d) 
            a[-4] += a[-2]
            # print(a[-4])
            a[-4] =  datetime.datetime.strptime(a[-4], "%Y-%m-%d%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
            # print(a[-4])
            a[-2] = a[2]+''+a[3]
            a[-5] = bool(a[-5])
            # print(a)
            del(a[5],a[5],a[5],a[6],a[7])
            # del[a[5],a[7]]
            print(a)

            query = """INSERT INTO branch_khasauliphc.base_app_user(id, user_name, first_name, middle_name, last_name, email,  password, is_superuser, gender, is_real_dob, pan_vat_no, photo, active, created_date_ad, created_date_bs, is_staff, is_email_verified, full_name, created_by_id)
                        VALUES(%s,%s,%s,'',%s,%s,%s,false,'',true,'',%s,%s,%s,%s,true,true,%s,%s)"""
            cur.execute(query,a)