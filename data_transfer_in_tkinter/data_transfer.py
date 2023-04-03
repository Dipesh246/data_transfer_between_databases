from tkinter import *
from tkinter import ttk
from import_data import import_mysql_data, export_mysql_data
from tkinter import messagebox

root = Tk()
root.title("Transfer data from MySQL to PostgreSQL")

root.geometry('500x300')

def transfer_data():
    data=import_mysql_data()
    rows = len(data)
    current_row = 0
    e = export_mysql_data()
    if not e:
        for d in data:
            export_mysql_data(d)
            current_row += 1
            progress.step(100/rows)
            root.update_idletasks()

        progress['value'] = progress['maximum'] = rows   
        
        messagebox.showinfo('Success', 'Data imported successfully!')
    else:
        messagebox.showinfo(e)


progress = ttk.Progressbar(root, orient=HORIZONTAL, length=380, mode='determinate')
progress.pack(pady=10)

btn = Button(root, text='Transfer', command=transfer_data)
btn.pack(pady=10)

root.mainloop()