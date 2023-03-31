from django.shortcuts import render
from django.db import connections
import datetime


def import_data(request):
    src_db = connections['src_db']
    dest_db = connections['default']

    with src_db.cursor() as cur:
        cur.execute("SELECT * FROM gl_tile_title")
        data = cur.fetchall()
    # print(data)
    with dest_db.cursor()as cu:
        for d in data:
            # print("\n",d)
            a= list(d)
            a[4] = a[4]+a[6]
            print("\n",a)
            a[4] = datetime.datetime.strptime(a[4],"%Y-%m-%d%H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
            del a[-2]
            a[3] = bool(a[3])
            
            print("\n",a)
            
            cu.execute('''INSERT INTO branch_khasauliphc.core_app_title(short_name, name, active, created_date_ad, created_date_bs, created_by_id, is_default, app_type_id, device_type_id)
                            VALUES(%s,%s,%s,%s,%s,%s,false,1,1)''',a[1:])
            dest_db.commit()
    
    return render(request,'tranfer.html')



