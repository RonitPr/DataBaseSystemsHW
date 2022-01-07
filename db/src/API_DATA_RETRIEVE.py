import csv
import mysql.connector
from environment import config, base_url
import urllib.request
import json

# utility functions


def get_list_of(data, data_key):
    l = []
    for i in data[data_key].split(', '):
        l.append(i)
    return l


def check_nulls(values):
    for value in values:
        if value == 'N/A' or value == '/N':
            return True
    return False


def get_runtime(str):
    for word in str.split():
        if word.isdigit():
            return word

# get data:


def get_csv_data(filename):
    # Write 100,000 movie imdb ids in a txt file from a given csv.
    count = 1
    try:
        with open(filename, 'r', encoding='latin-1') as csvFile:
            r = csv.reader(csvFile)
            with open('csv_movie_ids.txt', 'w') as output:
                for row in r:
                    if count > 100000:
                        break
                    if row[1] == 'movie' and row[5].isdecimal() and int(row[5]) >= 1990 and row[7] != "\\N":
                        output.write(f'{row[0]} ')
                        count += 1
    except:
        return "failed to get CSV data"


def get_main_tables_insert_queries(data, actor_list, director_list, genre_list):
    # return insert queries for movie, actor, director, genre in format:
    # [('query with %s',(values))]
    q = []
    q.append(('INSERT INTO movie (movie_id, title, year, rated, runtime, plot, imdb_rating) VALUES(%s,%s,%s,%s,%s,%s,%s);',
              (data['imdbID'], data['Title'],
               data['Year'], data['Rated'],
               get_runtime(data['Runtime']),
               data['Plot'],
               data['imdbRating'])))
    for genre in genre_list:
        q.append(('INSERT IGNORE INTO genre (name) VALUES (%s);', [genre]))
    for director in director_list:
        q.append(
            ('INSERT IGNORE INTO director (name) VALUES (%s);', [director]))
    for actor in actor_list:
        q.append(('INSERT IGNORE INTO actor (name) VALUES (%s);', [actor]))
    return q


def get_secondary_tables_insert_queries(con, data, actor_list, director_list, genre_list):
    # return insert queries for movie_actor, movie_director, movie_genre in format:
    # [('query with %s',(values))]
    q = []
    cursor2 = con.cursor(buffered=True)
    for genre in genre_list:
        cursor2.execute(
            "SELECT genre_id from genre where name = %s;", [genre])
        id = cursor2.fetchone()
        if id is None:
            break
        q.append(('INSERT INTO movie_genre (movie_id, genre_id) VALUES (%s,%s)',
                  (data['imdbID'], id[0])))
    for director in director_list:
        cursor2.execute(
            "SELECT director_id from director where name = %s;", [director])
        id = cursor2.fetchone()
        if id is None:
            break
        q.append(('INSERT INTO movie_director (movie_id, director_id) VALUES (%s,%s)',
                  (data['imdbID'], id[0])))
    for actor in actor_list:
        cursor2.execute(
            "SELECT actor_id from actor where name = %s;", [actor])
        id = cursor2.fetchone()
        if id is None:
            break
        q.append(('INSERT INTO movie_actor (movie_id, actor_id) VALUES (%s,%s)',
                  (data['imdbID'], id[0])))
    return q


def insert_single_movie(con, data):
    # with data that was checked for harmful nulls, execute inserts for mall tables
    al = get_list_of(data, 'Actors')
    dl = get_list_of(data, 'Director')
    gl = get_list_of(data, 'Genre')
    cursor = con.cursor(prepared=True)
    q = get_main_tables_insert_queries(data, al, dl, gl)
    for query, values in q:
        try:
            cursor.execute(query, values)
        except Exception as e:
            con.rollback()
            print(f'Failed to {query} with values: {values}. Rolling back.')
            raise e
    con.commit()

    q = get_secondary_tables_insert_queries(con, data, al, dl, gl)
    for query, values in q:
        try:
            cursor.execute(query, values)
        except Exception as e:
            con.rollback()
            print(
                f'Failed to {query} with values: {values}. Rolling back for secondary tables!!')
            raise e
    con.commit()


def insert_movies_batch(line):
    con = mysql.connector.connect(**config())
    count_insertions = 0
    ids_list = line.split(" ")
    for id in ids_list:
        if count_insertions % 10 == 0:
            print(f"Inserted {count_insertions} movies. Continuing...")
        params = urllib.parse.urlencode({'i': id})
        with urllib.request.urlopen(base_url() + params) as response:
            res = response.read()
            res = json.loads(res)
        if not check_nulls([res['Year'], res['Runtime'], res['imdbRating']]):
            try:
                insert_single_movie(con, res)
                count_insertions += 1
            except Exception as e:
                print(
                    f'Failed inserting movie {id} with exception: {e}. Skipping...')
    print('**********')
    print(f'Successfuly inserted {count_insertions} movies into DB.')
    print('**********')


if __name__ == '__main__':
    get_csv_data("imdb_data.csv")
    try:
        with open('csv_movie_ids.txt', 'r') as in_file:
            for line in in_file:
                stripped_line = line.strip()
                insert_movies_batch(stripped_line)
    except Exception as e:
        print(
            f'Failed to open the csv_movie_ids.txt file and insert the data: {e}')
