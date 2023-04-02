from tkinter import *
from import_data import import_data
from tkinter import messagebox

root = Tk()

root.title("Transfer data from MySQL to PostgreSQL")

root.geometry('300x300')
def data():
    import_data()
    
    messagebox.showinfo('Success', 'Data imported successfully!')

btn = Button(root, text='Import', command=data)





btn.pack()

root.mainloop()