import psycopg2
import mysql.connector

def connect_postgre():
    conn= psycopg2.connect(dbname='phl_cims', host='localhost', user='postgres', password='admin', port = 5432)
    conn.autocommit=True
    cur = conn.cursor()
    return cur

def connect_mysql():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="phl_cims",
    port = 3306
    )
    mydb.autocommit = True
    cur = mydb.cursor()
    return cur