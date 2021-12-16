# connect to the mysql db

import mysql.connector
from environment import config, base_url
import csv
import urllib.request
import json


def create_table(cursor, table_name, col_dict):
    columns = ""
    for key in col_dict:
        columns += "%s %s, " % (key, col_dict[key])
    columns = columns[:len(columns) - 2]
    print("CREATE TABLE %s (%s)" % (table_name, columns))  # delete later
    # cursor.execute("CREATE TABLE %s (%s)", t_name,columns)


# should we insert 1 at a time or collect many and send at once?
def insert_tuple_CSV(connection, cursor, file_name):
    with open(file_name, 'r', encoding='latin-1') as csv_file:
        r = csv.reader(csv_file)
        for row in r:
            if row[1] == "movie":
                print("%s, %s" % (row[0], row[2]))  # delete later
                # cursor.execute(
                #     "INSERT INTO movies (%s) VALUES (%s, %s)", (col_names, row[0], row[2]))
                # connection.commit()
                # print(cursor.rowcount, "record inserted.")
                # s = f"INSERT INTO movies ({col_names}) VALUES ({row[0]}, {row[2]})" TODO- convert to this

# create txt file with 2 delimeters- one for each 1000 and one for each id- TODO


# http????
def get_answers_json(url, values_dict):
    data = urllib.parse.urlencode(values_dict)
    with urllib.request.urlopen(url+data) as response:
        res = response.read()
        res = json.loads(res)

    print(res)


# con = mysql.connector.connect(**config())
# with con:
#     cursor = con.cursor()
create_table(
    0, "movies", {"tconst": "VARCHAR(255) PRIMARY KEY", "title": "VARCHAR(255)"})

# insert_tuple_CSV(0, 0, "imdb_data.csv")

get_answers_json(base_url(), {'i': 'tt3896198'})
