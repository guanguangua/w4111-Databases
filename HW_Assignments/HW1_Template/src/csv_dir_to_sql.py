import pymysql
import csv
import os
from os import listdir
from os.path import isfile, join
import shutil


_directory = "D:\\COMS4111\\w4111-Databases\\Data\\lahman2019"
_upload_dir = 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads'

cnx = pymysql.connect(host='localhost',
                             user='dbuser',
                             password='dbuserdbuser',
                             db='lahman2019raw',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def get_file_names(directory):
    onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f))]
    return onlyfiles


def get_load_info(directory, fn):
    rows = []
    full_name = os.path.join(directory, fn)
    with open(full_name, "r") as infile:
        rdr = csv.DictReader(infile)
        table_name = fn.split(".")
        table_name = table_name[0]
        columns = rdr.fieldnames
        for r in rdr:
            rows.append(r)

    return table_name, columns, rows


def create_table(table_name, columns):

    drop_sql = "drop table if exists " + table_name

    sql = "create table if not exists " + table_name + " ( "

    col_create = []
    for c in columns:
        col_create.append("`" + c + "` TEXT")

    all_cols = ", ".join(col_create)

    sql += all_cols + ") ENGINE=INNODB;"

    cur = cnx.cursor()
    cur.execute(drop_sql)
    print("SQL = ", sql)
    res = cur.execute(sql)
    cnx.commit()
    return res

def load_data(table_name, columns, rows):

    cols = ["`" + c + "`" for c in columns]
    cols = ",".join(cols)
    cols = "(" + cols + ") "
    vals = ["%s" for c in columns]
    vals = " values(" + ",".join(vals) + ") "
    sql = "insert into " + table_name + " " + cols + vals

    cur = cnx.cursor()
    cnt = 0
    for r in rows:
        in_vals = [r[c] for c in columns]
        res = cur.execute(sql, in_vals)
        cnt = cnt + 1
    cnx.commit()
    return cnt



def driver():
    file_names = get_file_names(_directory)
    for f in file_names:
        print("Starting load for file = ", os.path.join(_upload_dir, f))
        l_info = get_load_info(_directory, f)
        create_table(l_info[0], l_info[1])
        cnt = load_data(l_info[0], l_info[1], l_info[2])
        print("Loaded ", str(cnt), "rows into", l_info[0])


driver()
