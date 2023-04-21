import datetime
from psycopg2 import IntegrityError
from connection import connect_mysql,connect_postgre

class Databasehandler:
    def __init__(self,src_table,dest_table):
        self.src_table = src_table
        
        self.dest_table = dest_table
        

    def mysql_tables():
        db = connect_mysql()
        with db.cursor() as cur:
            cur.execute("SHOW TABLES")
            tables = cur.fetchall()
        
        cur.close()
        db.close()

        return tables

    def postgresql_tables():
        db = connect_postgre()
        with db.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'branch_khasauliphc'")  
            tables=cur.fetchall()

            
        cur.close()
        db.close()
        return tables      

    def import_mysql_data(self):
        src_db = connect_mysql()
        # t =[self]
        # print(t)     
        with src_db.cursor() as cur:
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE (table_schema='phl_cims') AND (table_name = '{self.src_table}') ORDER BY ordinal_position; ")
            pk = cur.fetchall()[0]

            query = f"""SELECT * FROM {self.src_table} WHERE is_migrated=false ORDER BY {pk[0]} ;"""
            
            cur.execute(query)
            data = cur.fetchall()
            
            src_db.commit()
        cur.close()
        src_db.close()
        return data
        
    def export_mysql_data(self,src_data):
        dest_db = connect_postgre()
        # print('called')
        dest_db.autocommit=False
        src_db = connect_mysql()
        mcur= src_db.cursor()
        
        try:
            with dest_db.cursor() as cur:
                if(self.dest_table =='base_app_user' ):
                    a = list(src_data) 
                    # print(a)
                    a[-4] += a[-2]
                    # print(a[-4])
                    a[-4] =  datetime.datetime.strptime(a[-4], "%Y-%m-%d%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                    # print(a[-4])
                    a[-2] = a[2]+''+a[3]
                    a[-5] = bool(a[-5])
                    # print(a)
                    del(a[5],a[5],a[5],a[6],a[7])
                    # del[a[5],a[7]]
                    # print(a)

                    query = f"""INSERT INTO branch_khasauliphc.base_app_user(id, user_name, first_name, middle_name, last_name, email,  password, is_superuser, gender, is_real_dob, pan_vat_no, photo, active, created_date_ad, created_date_bs, is_staff, is_email_verified, full_name, created_by_id)
                                VALUES({a[0]},'{a[1]}','{a[2]}','','{a[3]}','{a[4]}','{a[5]}',false,'',true,'','{a[6]}','{a[7]}','{a[8]}','{a[9]}',true,true,'{a[10]}','{a[11]}')"""
                    cur.execute(query)
                    # dest_db.commit()
                
                elif(self.dest_table=='core_app_title'):
                    a= list(src_data)
                    a[4] = a[4]+a[6]
                    # print("\n",a)
                    a[4] = datetime.datetime.strptime(a[4],"%Y-%m-%d%H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
                    del a[-2]
                    a[3] = bool(a[3])
                    
                    # print("\n",a)
                    
                    cur.execute('''INSERT INTO branch_khasauliphc.core_app_title(id,short_name, name, active, created_date_ad, created_date_bs, created_by_id, is_default, app_type_id, device_type_id)
                                    VALUES(%s,%s,%s,%s,%s,%s,%s,false,1,1)''',a)
                    # dest_db.commit()
                    
                elif(self.dest_table=='base_app_usergroup'):
                    a= list(src_data)
                    a[4] = a[4]+a[6]
                    # print("\n",a)
                    a[4] = datetime.datetime.strptime(a[4],"%Y-%m-%d%H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
                    
                    a[3] = bool(a[3])

                    print("\n",a)

                    cur.execute(f"""INSERT INTO branch_khasauliphc.base_app_usergroup(id,name,code,created_date_ad,created_date_bs,is_system_managed,active,app_type_id,created_by_id,device_type_id)
                                   VALUES({a[0]},'{a[1]}','{a[1][:5]}','{a[4]}','{a[5]}',true,{bool(a[3])},1,{a[7]},1)""")
                    # dest_db.commit()

                elif(self.dest_table=='doctor_doctor'):
                    print("reached")
                    a= list(src_data)
                    a[11] = a[11]+a[13]
                    print("\n",a)
                    a[11] = datetime.datetime.strptime(a[11],"%Y-%m-%d%H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
                    cur.execute(f"""INSERT INTO branch_khasauliphc.base_app_user(user_name, first_name, middle_name, last_name, email,password,is_superuser,gender,is_real_dob,pan_vat_no,photo,active,created_date_ad,created_date_bs,is_staff,is_email_verified,full_name,created_by_id,person_title_id)
                                VALUES('{a[2]+a[4]}','{a[2]}','{a[3]}','{a[4]}','{a[5]}','password',false,'',true,'','{a[9]}','{a[10]}','{a[11]}','{a[12]}',true,true,'{a[2]} {a[3]} {a[4]}',{a[14]},{a[15]})
                                RETURNING id""")
                    fk=cur.fetchone()[0]
                    print(fk)
                    cur.execute(f"""INSERT INTO branch_khasauliphc.doctor_doctor(id,created_date_ad,created_date_bs,doctor_type,nmc_no,qualification,specialization,experience,app_type_id,created_by_id,device_type_id,user_id)
                                    VALUES({a[0]},'{a[11]}','{a[12]}','OPD','{a[8]}','','',0,1,{a[14]},1,{fk})""")
                    # dest_db.commit()
                
                elif(self.dest_table=='test_department'):
                    a= list(src_data)
                    a[5] = a[5]+a[7]
                    print("\n",a)
                    a[5] = datetime.datetime.strptime(a[5],"%Y-%m-%d%H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
                    cur.execute(f"""INSERT INTO branch_khasauliphc.test_department(id,created_date_ad,created_date_bs,name,description,icon,code,department_type,requires_sample,display_order,is_system_managed,active,app_type_id,created_by_id,device_type_id)
                                    VALUES({a[0]},'{a[5]}','{a[6]}','{a[2]}','','','{a[1]}','GENERAL',false,{a[3]},false,{bool(a[4])},1,{a[8]},1)""")
                    
                    # dest_db.commit()

                elif(self.dest_table=='core_app_province'):
                     a= list(src_data)
                     cur.execute(f"""INSERT INTO branch_khasauliphc.core_app_province(id,created_date_ad,created_date_bs,name,is_default,active,app_type_id,created_by_id,device_type_id)
                                    VALUES({a[0]},'2019/4/6 12:16:15','2076/12/23','{a[1]}',false,{bool(a[3])},1,{a[4]},1)""")
                    #  dest_db.commit() 
                
                elif(self.dest_table=='core_app_district'):
                    a=list(src_data)
                    cur.execute(f"""INSERT INTO branch_khasauliphc.core_app_district(id,created_date_ad,created_date_bs,name,is_default,active,app_type_id,created_by_id,device_type_id,province_id)
                                    VALUES({a[0]},'2023/04/06 15:24:15','2079/12/23','{a[1]}',true,{bool(a[4])},1,{a[8]},1,{a[2]})""")
                    

                elif(self.dest_table=='clinic_setup_referrer'):
                    a= list(src_data)
                    a[3] = a[3]+a[5]
                    
                    # print("\n",b)
                    
                    # print(a[1].replace(" ",""))
                    
                    a[3] = datetime.datetime.strptime(a[3],"%Y-%m-%d%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                    cur.execute(f"""INSERT INTO branch_khasauliphc.base_app_user(user_name, first_name, middle_name, last_name, email,password,is_superuser,gender,is_real_dob,pan_vat_no,photo,active,created_date_ad,created_date_bs,is_staff,is_email_verified,full_name,created_by_id)
                                VALUES('{a[1][6:].replace(" ",".")}{a[0]}','{a[1]}','','','','password',false,'',true,'','','{bool(a[2])}','{a[3]}','{a[4]}',true,true,'{a[1]}',{a[6]})
                                RETURNING id""")
                    fk=cur.fetchone()[0]
                    print(fk)
                    cur.execute(f"""INSERT INTO branch_khasauliphc.clinic_setup_referrer(id,created_date_ad,created_date_bs,commission_rate,commission_amount,app_type_id,created_by_id,device_type_id,user_id)
                                    VALUES({a[0]},'{a[3]}','{a[4]}',0.00,0.00,1,{a[6]},1,{fk})""")
                    
                
                elif(self.dest_table=='customer_customer'):
                    a= list(src_data)
                    # a = [None if x==0 else x for x in a]
                    yerad=int(a[20][1:4])
                    yerbs=int(a[21][1:4])
                    mnth=int(a[20][4:7]) 
                    if mnth >= 4:
                        fiscalbs=f"{yerbs}/{yerbs+1}"
                        fiscalad=f"{yerad}/{yerad+1}"
           
                    else:
                        fiscalbs =f"{yerbs-1}/{yerbs}"
                        fiscalad =f"{yerad-1}/{yerad}"

                    a[20] = a[20]+a[22]
                    
                    
                    # print("\n",b)
                    
                    # print(a[1].replace(" ",""))
                    
                    a[20] = datetime.datetime.strptime(a[20],"%Y-%m-%d%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                    cur.execute(f"""INSERT INTO branch_khasauliphc.base_app_user(user_name, first_name, middle_name, last_name, email,password,is_superuser,gender,is_real_dob,pan_vat_no,photo,active,created_date_ad,created_date_bs,is_staff,is_email_verified,full_name,created_by_id,mobile_no,phone_no,dob_date_ad,dob_date_bs,person_title_id)
                                VALUES('{a[2]}{a[0]}','{a[2]}','{a[3]}','{a[4]}','{a[9]}','password',false,'{a[5]}',true,'{a[10]}','','{bool(a[19])}','{a[20]}','{a[21]}',true,true,'{a[2]} {a[3]} {a[4]}',{a[23]},'{a[7]}','{a[8]}','{a[16]}','{a[17]}','{a[1]}')
                                RETURNING id""")
                    fk=cur.fetchone()[0]
                    # print(fk)
                    cur.execute(f"""INSERT INTO branch_khasauliphc.customer_customer(id,created_date_ad,created_date_bs,prefix,separator,fiscal,customer_no,customer_no_full,direct_customer,plain_password,walk_in,marital_status,camera_photo,app_type_id,created_by_id,customer_type_id,department_id,device_type_id,doctor_id,fiscal_session_ad_id,fiscal_session_bs_id,user_id)
                                    SELECT {a[0]},'{a[20]}','{a[21]}','','','{fiscalbs}',{a[0]},'{a[0]}',{bool(a[18])},'password',false,'n/a','',1,{a[23]},1,CAST(NULLIF({a[13]},0) AS integer),1,CAST(NULLIF({a[14]},0) AS integer),core_app_fiscalsessionad.id,core_app_fiscalsessionbs.id,{fk}
                                    FROM branch_khasauliphc.core_app_fiscalsessionad,branch_khasauliphc.core_app_fiscalsessionbs
                                    WHERE core_app_fiscalsessionad.session_short= '{fiscalad}' AND core_app_fiscalsessionbs.session_short='{fiscalbs}';""")
                    
                    mcur.execute(f"UPDATE {self.src_table} SET is_migrated=true WHERE cuer_customerid={a[0]}")
                    
                    
                    

                    
                dest_db.commit()
        except IntegrityError as e:
            dest_db.rollback()
            
            return e
        except Exception as e:
            dest_db.rollback()
            return e

        cur.close()
        dest_db.close()  