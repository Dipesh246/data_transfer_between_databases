from tkinter import *
from tkinter import ttk
from import_data import Databasehandler
from tkinter import messagebox

mysql_tables = Databasehandler.mysql_tables()

post_tables = Databasehandler.postgresql_tables()

root = Tk()
root.title("Transfer data from MySQL to PostgreSQL")

root.geometry('700x500')
sl = Label(root,text="Select table to get data from:").place(x=40,y=20)
sl = Label(root,text="Select table to insert data into:").place(x=480,y=20)

val1 = StringVar()
val2 = StringVar()

cb1 = ttk.Combobox(root, textvariable=val1)
cb1['values'] = [x for x in mysql_tables]

cb1.place(x=60,y=45)



cb2= ttk.Combobox(root,textvariable=val2)
cb2['values'] = [x for x in post_tables]
cb2.place(x=485,y=45)


def on_select_mysql(event):
    global mysql_table
    mysql_table = cb1.get()
    # print(mysql_table)

def on_select_postgre(event):
    global post_table
    post_table =cb2.get()

cb1.bind('<<ComboboxSelected>>',on_select_mysql)
cb2.bind('<<ComboboxSelected>>',on_select_postgre)

dh=None


def transfer_data():
    global dh
    dh =Databasehandler(mysql_table,post_table)
    data = dh.import_mysql_data()
    rows = len(data)
    # print(data)
    current_row = 0

    for d in data[3:]:
        # print(d)
        e = dh.export_mysql_data(d)
        if not e:
            current_row += 1
            progress.step(100/rows)
            root.update_idletasks()
        else:
            print(e)
            messagebox.showerror('data transfer failed',e)
            break

    progress['value'] = progress['maximum'] = current_row

    if not e:
        messagebox.showinfo('Success', 'Data imported successfully!')
    


progress = ttk.Progressbar(root, orient=HORIZONTAL,
                           length=580, mode='determinate')
progress.place(x=60 ,y=100)

btn = Button(root, text='Transfer', command=transfer_data)
btn.place(x=280,y=150)

root.mainloop()
