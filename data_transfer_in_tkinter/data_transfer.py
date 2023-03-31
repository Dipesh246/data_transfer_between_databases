from tkinter import *

from tkinter.ttk import *
from connection import connect_mysql,connect_postgre

root = Tk()

root.title("Transfer data from MySQL to PostgreSQL")

root.geometry('300x300')

def import_data():
    src_db = connect_mysql()
    dest_db = connect_postgre()



btn = Button(root, text='Import', command=import_data())




btn.pack()

root.mainloop()